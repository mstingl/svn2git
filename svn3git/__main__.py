import json
import os
from functools import partial
from typing import Optional
from urllib.parse import urlparse

import questionary
import typer
from git import Repo
from git.cmd import handle_process_output
from git.exc import GitCommandError, InvalidGitRepositoryError, NoSuchPathError
from rich import print
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.prompt import Prompt
from rich.table import Column

from .converter import Converter
from .exceptions import MergeError, MissingAuthorError, MissingBranchError, SvnExternalError, SvnFetchError, TagExistsError
from .utils import *


def do_update(
    svn_url: Optional[str] = None,
    git_url: Optional[str] = None,
    folder: Optional[str] = None,
    svn_stdlayout: bool = True,
    svn_trunk: Optional[str] = None,
    svn_branches: Optional[str] = None,
    svn_tags: Optional[str] = None,
    svn_revision_start: int = 0,
    svn_revision_stop: Optional[int] = None,
    skip_trunk: bool = True,
    push_force: bool = False,
    svn_fetch: bool = True,
    git_fetch: bool = True,
    git_push: bool = True,
    lfs: Optional[bool] = None,
    lfs_above: str = '1MB',
    lfs_filetypes: Optional[str] = None,
    migrate_externals_to_submodules: bool = True,
    confirmations: bool = True,
    show_externals: bool = False,
):
    if push_force:
        print("[bold red]Force push is enabled[/bold red]")
        if confirmations and not typer.confirm("Do you want to continue?"):
            return

    if svn_url or git_url:
        try:
            Repo(os.getcwd())

        except InvalidGitRepositoryError:
            pass

        else:
            print(f"[yellow]Currently inside a git repository[/yellow]")
            return

    if not folder:
        folder = os.path.join(os.getcwd(), urlparse(svn_url).path.strip("/").split("/")[-1]) if svn_url else os.getcwd()

    try:
        repo = Repo(folder)

    except (InvalidGitRepositoryError, NoSuchPathError):
        if not svn_stdlayout and not (svn_trunk and svn_branches and svn_tags):
            progress.stop()
            print("[red]If stdlayout is not used, [b]--svn-trunk[/b], [b]--svn-branches[/b] and [b]--svn-tags[/b] must be given[/red]")
            return

        elif svn_stdlayout and (svn_trunk and svn_branches and svn_tags):
            print("[red]If stdlayout is used, [b]--svn-trunk[/b], [b]--svn-branches[/b] and [b]--svn-tags[/b] must not be given[/red]")
            return

        repo = Repo.init(folder)

    if repo.config_reader().has_section('svn-remote "svn"'):
        config_svn_url = repo.config_reader().get_value('svn-remote "svn"', 'url')
        if svn_url and config_svn_url.removesuffix('/') != svn_url.removesuffix('/'):
            print("[red]SVN remote is already defined in repository and given url is different from the configured![/red]")
            return

        svn_url = config_svn_url

    elif svn_url:
        repo.git.svn(
            "init",
            svn_url.strip(),
            stdlayout=svn_stdlayout,
            trunk=svn_trunk,
            branches=svn_branches,
            tags=svn_tags,
            prefix="svn/",
        )

        # On first run for a repo, do not try to fetch git repo
        git_fetch = False
        push_force = True

    else:
        print("[red]SVN path not defined in repo and not declared. Pass SVN url using [b]--svn-url[/b][/red]")
        return

    print("[green]Using SVN remote %s" % svn_url)

    if os.path.exists(os.path.join(repo.working_dir, '.gitattributes')):
        print(
            "[red]A .gitattributes file exists, which usually indicates that LFS was used. Fetching new commits from SVN is not possible when the GIT repository was migrated to LFS.[/red]"
        )
        if confirmations and not typer.confirm("Are you sure you want to continue?"):
            return

    if not repo.remotes and git_url:
        repo.create_remote("origin", git_url.strip())

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(table_column=Column(justify='center')),
        TaskProgressColumn(),
        TimeRemainingColumn(),
        TimeElapsedColumn(),
    ) as progress:
        converter = Converter(
            repo,
            force=push_force,
            log=progress.log,
            skip_trunk=skip_trunk,
        )

        if show_externals:
            progress.stop()
            print(converter.get_unlinked_externals()[1])
            return

        if repo.remotes:
            if git_fetch:
                with task(progress, "Fetching updates from GIT") as task_id:
                    repo.remotes.origin.fetch(
                        progress=partial(git_progress, progress=progress, task_id=task_id),
                    )

            elif not push_force:
                progress.stop()
                print("[red]Enable [b]--push-force[/b] to override git repository without fetching new updates[/red]")
                return

        else:
            print("[yellow]WARNING: No GIT origin defined, the repository will not be pushed automatically")
            progress.stop()
            if confirmations and not typer.confirm("Do you want to continue?"):
                return

            progress.start()

        if svn_fetch:
            with task(progress, "Fetching updates from SVN") as task_id:
                try:
                    converter.svn_fetch(
                        start=svn_revision_start,
                        stop=svn_revision_stop,
                        progress=lambda d: progress.update(task_id, **d),
                    )

                except MissingAuthorError as error:
                    progress.stop()
                    progress.print("[bold orange_red1]Missing Author:[/bold orange_red1] " + error.args[0])
                    return

                except SvnFetchError as error:
                    progress.stop()
                    print("[bold red]ERROR:[/bold red] " + '\n'.join(error.args[0]))
                    return

                progress.advance(task_id)

        if 'master' in repo.branches:
            with task(progress, "Remove old master branch"):
                if 'main' not in repo.branches:
                    repo.create_head('main', 'master')

                repo.branches["main"].checkout(force=True)

                repo.delete_head('master', force=True)

        with task(progress, "Updating branches", total=len(converter.references) + 1) as task_id:
            try:
                for ref in converter.update_refs():
                    progress.advance(task_id)

            except TagExistsError as error:
                progress.stop()
                print(
                    "[red]Tag [b]%s[/b] already exists (on [i]%s[/i] instead of [i]%s[/i]). If you actually want to override, pass [b]--push-force[/b][/red]"
                    % (error.tag.name, error.tag.commit.hexsha, error.commit.hexsha)
                )
                return

            except MergeError as error:
                progress.stop()
                print("[red][b]Failed to merge[/b]: %s[/red]" % error.__cause__)
                return

            except MissingBranchError:
                progress.stop()
                print("[red]No [b]main[/b] branch existing, is the SVN repo using the standardlayout? If not, retry with [b]--no-stdlayout[/b][/red]")
                return

        if not lfs_filetypes and os.path.exists(os.path.join(repo.working_dir, '.gitattributes')):
            lfs = True
            with open(os.path.join(repo.working_dir, '.gitattributes')) as gitattributes:
                lfs_filetypes = ",".join(
                    [
                        attribute.split(' ', 1)[0]  # primitive checking, paths with spaces would fail, but should not be used in general
                        for attribute in gitattributes.readlines()
                        if 'filter=lfs' in attribute and 'diff=lfs' in attribute and 'merge=lfs' in attribute
                    ]
                )

        if not lfs_filetypes and lfs is not False:
            with task(progress, "Analyzing repository for large files"):
                lfs_info = repo.git.lfs("migrate", "info", "--everything", "--top=100", "--pointers=ignore", f"--above={lfs_above}").splitlines()

            if len(lfs_info) > 2:
                filetypes = [[f.strip() for f in filetype.split("\t")] for filetype in lfs_info[:-2]]
                lfs_filetypes = ",".join(f[0] for f in filetypes)

                if lfs is None:
                    progress.stop()
                    print(
                        f"""[bold orange_red1] Found large files ({lfs_filetypes}) eligable for LFS.[/bold orange_red1]
 Start this script again and pass [b]--lfs[/b] if you want to migrate them or [b]--no-lfs[/b] if you want to keep them regulary in the repository.
 [yellow]WARNING: after migration to LFS, a new fetch from SVN will not be possible, as the history is being altered on LFS migration![/yellow]
 If you want to declare different filetypes as the suggestes ones, give [b]--lfs-filetypes[/b] with a comma separated list of fileendings.
 If the repository was already pushed without LFS to a git remote, you have to enable force push using [b]--push-force[/b].
"""
                    )
                    return

        if lfs and lfs_filetypes:
            with task(progress, f"Migrating to LFS"):
                process = repo.git.lfs("migrate", "import", "--everything", f'--include={lfs_filetypes}', as_process=True)
                handle_process_output(process, stdout_handler=lambda message: progress.log(message.strip('\n')), stderr_handler=None)
                process.wait()

        with task(progress, "Retrieving additional SVN options", total=1) as task_id:
            try:
                try:
                    svn_externals = converter.get_externals()  # preload data into cache

                except ValueError as error:
                    progress.stop()
                    print("[red]Cannot parse external: %s[/red]" % error.args[0])
                    return

                progress.advance(task_id)

            except GitCommandError:
                progress.stop()
                print("[red][b]ERROR[/b]: Cannot get options from SVN[/red]")
                return

        with task(progress, "Migrate ignored files to GIT") as task_id:
            converter.migrate_gitignore(cherrypick_progress=lambda d: progress.update(task_id, **d))

        if any(svn_externals.keys()):
            progress.stop()
            print("[yellow][b]WARNING[/b]: This SVN repository contains externals to [b]other repositories[/b][/yellow]")

            try:
                (
                    external_repos_unlinked,
                    external_paths_table,
                    submodules_temp_config,
                ) = converter.get_unlinked_externals()

            except SvnExternalError as error:
                print("[red]Cannot work with external path [b]%s[/b][/red]" % error.args[0])
                return

            print(external_paths_table)

            if migrate_externals_to_submodules:
                for svn_repo_name, paths in external_repos_unlinked.items():
                    print(f"[b]{svn_repo_name}[/b]")

                    choices = [
                        questionary.Choice(f"{svn_repo_name}/{repo_path} ({branch or 'main'}) <- {local_path}", i)
                        for i, (repo_path, local_path, branch) in enumerate(paths)
                    ]

                    question = questionary.checkbox(
                        "Select folders which belong to the same repository in git",
                        choices=choices,
                        validate=lambda c: len(set(paths[i][2] for i in c)) == 1,
                    )
                    while not all(c.disabled for c in choices):
                        if len(choices) > 1:
                            selected = question.ask()
                            if selected is None:
                                return

                        else:
                            selected = [0]

                        selected_paths = []
                        for i in selected:
                            print(f"- {choices[i].title}")
                            selected_paths.append(paths[i])

                        default_branch = selected_paths[0][2]
                        if default_branch and '@' in default_branch:
                            default_branch, revision = default_branch.split('@')

                        branch = Prompt.ask("Chose branch (empty to use default)", default=default_branch)
                        commit = None
                        if revision:
                            while not (commit := Prompt.ask(f"Enter commit hash for revision [b]{revision}[/b]")):
                                pass

                        git_external_url = None
                        while True:
                            git_external_url = Prompt.ask(f"Input GIT url for repository (on [b]{branch or 'main'}[/b])")
                            if git_external_url:
                                break

                            if typer.confirm("Are you sure to skip this external?"):
                                submodules_temp_config[svn_repo_name] = {'do_skip': True}
                                break

                        for i in selected:
                            choices[i].disabled = git_external_url or "(-)"

                        if not git_external_url:
                            continue

                        common_path_replacement = ""
                        common_path_prefix = os.path.commonprefix([path[0] for path in selected_paths])
                        if not common_path_prefix.endswith("/"):
                            common_path_prefix = ""

                        if not common_path_prefix:
                            common_path_prefix = os.path.dirname(selected_paths[0][0]) + "/"

                        if common_path_prefix and typer.confirm(f"Should the common prefix ({common_path_prefix}) be removed or changed?"):
                            common_path_replacement = Prompt.ask("Enter replacement or leave empty to remove")
                            if common_path_replacement:
                                common_path_replacement = common_path_replacement.removesuffix('/') + '/'

                        git_repo_name = os.path.basename(git_external_url).removesuffix('.git')
                        submodule_path = os.path.join("external_repos", git_repo_name)
                        if typer.confirm(f"Do you want to change the path ({submodule_path}) on which the submodule will be created?"):
                            submodule_path = Prompt.ask("Enter new path for the submodule")

                        submodule_key = f"{svn_repo_name}>{git_repo_name}"
                        if submodule_key in submodules_temp_config:
                            print("[red]Overlapping configuration, exiting! Please start again")
                            return

                        submodules_temp_config[submodule_key] = {
                            'repo_name': git_repo_name,
                            'svn_repo_name': svn_repo_name,
                            'submodule_path': submodule_path,
                            'git_external_url': git_external_url,
                            'branch': branch,
                            'commit': commit,
                            'common_path_replacement': common_path_replacement,
                            'common_path_prefix': common_path_prefix,
                            'paths': selected_paths,
                        }

                with open(converter.svn_externals_to_git_config_file, 'wb') as config_file:
                    config_file.write(bytes(json.dumps(submodules_temp_config), 'utf-8'))

            progress.start()

            if migrate_externals_to_submodules:
                with task(progress, "Create Submodules", total=len(submodules_temp_config)) as task_id:
                    with task(
                        progress, "Updating branches", total=len(submodules_temp_config), remove_after_complete=True, start=False
                    ) as cherrypick_task_id:
                        for _ in converter.migrate_externals_to_submodules(
                            submodules_temp_config,
                            cherrypick_progress=lambda d: (progress.start_task(cherrypick_task_id), progress.update(cherrypick_task_id, **d)),
                        ):
                            progress.advance(task_id)

                os.remove(converter.svn_externals_to_git_config_file)

        if repo.remotes and git_push and converter.refs_to_push:
            with task(progress, "Push to Git", indeterminate=False) as task_id:
                progress.update(task_id, total=len(converter.refs_to_push))
                for ref in converter.refs_to_push:
                    with task(
                        progress,
                        "Pushing %s" % ref,
                        indeterminate=False,
                        remove_after_complete=True,
                    ) as ref_push_task_id:
                        for _try in range(3):
                            try:
                                repo.remotes.origin.push(
                                    ref,
                                    progress=partial(git_progress, progress=progress, task_id=ref_push_task_id),
                                    force=push_force,
                                )
                                progress.advance(task_id)

                            except GitCommandError as error:
                                if _try < 2:
                                    continue

                                progress.stop()
                                print("[red][b]ERROR[/b] Git failed to push (%s): %s[/red]" % (error.status, error.stderr))
                                return

                            else:
                                break

        if not all(svn_externals.keys()):
            progress.stop()
            print(
                "[yellow][b]WARNING[/b]: This SVN repository contains externals with [b]local links[/b]. Those were [b]not[/b] migrated automatically. To display all externals, use [b]--show-externals[/b][/yellow]"
            )


if __name__ == "__main__":
    typer.run(do_update)
