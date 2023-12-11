import json
import os
import re
import shutil
from collections import defaultdict
from functools import cache, partial
from typing import Callable, Optional

from git import Actor, Head, RemoteReference, Repo
from git.cmd import handle_process_output
from git.exc import GitCommandError
from rich import print
from rich.progress import Progress
from rich.table import Column, Table
from svn.remote import RemoteClient

from .exceptions import MergeError, MissingAuthorError, MissingBranchError, SvnExternalError, SvnFetchError, SvnOptionsReadError, TagExistsError
from .utils import cherrypick_to_all_branches, cherrypick_to_all_branches_with_progress, git_svn_show, reference_name, rmtree_error_handler


class Converter:
    def __init__(self, repo: Repo, force: bool = False, log: Optional[Callable[[str], None]] = None, skip_trunk: bool = True):
        self.repo = repo
        self.working_dir = self.repo.working_dir
        self.force = force
        self.refs_to_push = set()
        self.log = log or print
        self.skip_trunk = skip_trunk
        self.svn_externals_to_git_config_file = os.path.join(self.working_dir, '.svn_externals_to_git_submodules.json')
        self.svn_url = self.repo.config_reader().get_value('svn-remote "svn"', 'url')

    @property
    def references(self):
        return [reference for reference in self.repo.references if isinstance(reference, (RemoteReference, Head)) and '@' not in reference.name]

    def svn_fetch(self, start: Optional[int] = None, stop: Optional[int] = None, progress: Callable[[dict], None] = lambda d: None):
        svn_client = RemoteClient(self.svn_url)
        svn_latest_revision = svn_client.info()['entry_revision']

        if not stop or stop > svn_latest_revision:
            stop = svn_latest_revision

        progress({'completed': start, 'total': stop})

        def svn_log_handler(message: str):
            self.log(message.strip('\n'))
            if message.startswith('r') and (match := re.match(r'^r(\d+) = (\w+)( \(.*\))?$', message)):
                # the message states the revision number which will be migrated, so it is not completed yet
                progress({"completed": int(match.group(1)) - 1})

        def svn_err_handler(message: str, log):
            log.append(message.strip('\n'))

        while True:
            svn_fetch_stderr = []
            try:
                process = self.repo.git.svn("fetch", f"--revision={start}:{stop}", "--ignore-refs=tags\/https?:", as_process=True)
                handle_process_output(process, stdout_handler=svn_log_handler, stderr_handler=partial(svn_err_handler, log=svn_fetch_stderr))
                process.wait()

            except GitCommandError as error:
                if svn_fetch_stderr and "Author:" in svn_fetch_stderr[-2]:
                    raise MissingAuthorError(svn_fetch_stderr[-2]) from error

                raise SvnFetchError(svn_fetch_stderr) from error

            else:
                break

    def update_refs(self):
        for reference in self.references:
            if isinstance(reference, (RemoteReference)) and reference.remote_head.startswith("tags/"):
                tag_name = reference.remote_head.removeprefix("tags/")
                tag = next((tag for tag in self.repo.tags if tag_name == tag.name), None)
                commit = reference.commit
                while not commit.stats.files:
                    commit = commit.parents[0]

                if tag:
                    if tag.commit.hexsha == commit.hexsha:
                        yield
                        continue

                    if not self.force:
                        raise TagExistsError(tag, commit)

                    self.repo.delete_tag(tag_name)
                    self.log("Deleted tag %s" % tag_name)

                tag = self.repo.create_tag(
                    tag_name,
                    ref=commit,
                )

                self.refs_to_push.add(tag.path)
                self.log("Created tag %s" % tag)

            else:
                branch_name = reference_name(reference)
                if self.skip_trunk and branch_name == "trunk":
                    branch_name = "main"

                branch = next(
                    (b for b in self.repo.branches if b.name == branch_name),
                    None,
                )
                if not branch or self.force:
                    self.log(f"Creating new branch {branch_name} from {reference.commit.hexsha}")
                    branch = self.repo.create_head(
                        branch_name,
                        reference.commit,
                        force=bool(branch and self.force),
                    )
                    branch.checkout(force=True)

                else:
                    if branch.commit.hexsha != reference.commit.hexsha:
                        self.log("Updating branch %s" % branch_name)
                        branch.checkout(force=True)

                        try:
                            self.repo.git.merge(reference.commit.hexsha, '--ff')

                        except GitCommandError as error:
                            raise MergeError from error

                self.refs_to_push.add(branch.path)

            yield

        try:
            self.repo.branches["main"].checkout(force=True)
            yield

        except IndexError:
            raise MissingBranchError("main")

    def migrate_gitignore(self, progress: Optional[Progress] = None):
        try:
            svn_ignore = git_svn_show(self.repo, "ignore")

        except GitCommandError as error:
            raise SvnOptionsReadError from error

        if not svn_ignore:
            return

        gitignore_path = os.path.join(self.working_dir, '.gitignore')
        if os.path.exists(gitignore_path):
            with open(gitignore_path, 'r') as gitignore:
                current_gitignore = [line.strip('\n').strip('\r') for line in gitignore.readlines()]

        else:
            current_gitignore = []

        if ignores_to_add := [
            filepath
            for filepath in svn_ignore
            if self.repo.git.check_ignore(self.working_dir + filepath, no_index=True, with_exceptions=False, with_extended_output=True)[0] != 0
            and not filepath in current_gitignore
        ]:
            self.log("[cyan]Updating [b].gitignore[/b][/cyan]")
            with open(gitignore_path, 'a') as gitignore:
                gitignore.write(os.linesep + os.linesep.join(["# migrated from SVN"] + ignores_to_add) + os.linesep)

            self.repo.index.add(['.gitignore'])
            commit = self.repo.index.commit("Migrate svn:ignore to .gitignore", skip_hooks=True, author=Actor("svn2git", "svn2git@example.com"))
            if progress:
                cherrypick_to_all_branches_with_progress(self.repo, commit, progress, self.refs_to_push)

            else:
                cherrypick_to_all_branches(self.repo, commit, self.refs_to_push)

    @cache
    def get_externals(self):
        svn_externals_repos = []
        svn_externals_local_symlinks = []
        external: str
        for external in git_svn_show(self.repo, "externals"):
            match = re.match(
                r"^(\/(?P<path_base>.+?))?\/((?P<url>(\w+):\/\/[^ @]+)|(\^\/(?P<current>[^ @]+))|(?P<parent>\.\.\/[^ @]+))(@(?P<revision>\d+))? (?P<path_local>.*)$",
                external,
            )
            if not match:
                raise ValueError(external)

            path = (f"{match.group('path_base')}/" if match.group('path_base') else "") + match.group('path_local')

            if match.group('url'):
                svn_externals_repos.append((match.group('url'), path, match.group('revision')))

            elif match.group('parent'):
                svn_externals_local_symlinks.append((os.path.relpath(os.path.join(path, match.group('parent'))), path, match.group('revision')))

            elif match.group('current'):
                svn_externals_local_symlinks.append((match.group('current'), path, match.group('revision')))

            else:
                raise NotImplementedError

        return svn_externals_repos, svn_externals_local_symlinks

    def get_submodules_temp_config(self) -> dict:
        try:
            with open(self.svn_externals_to_git_config_file, 'rb') as config_file:
                return json.loads(str(config_file.read(), 'utf-8')) or {}

        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def svn_path_to_branch(self, svn_path, revision):
        path_parts = svn_path.strip('/').split('/')
        if not path_parts:
            # probably obsolete as branches are now handled per path
            raise NotImplementedError

        elif path_parts[0] == "trunk":
            branch = ""  # default / main
            repo_path_prefix = "trunk"

        elif path_parts[0] == 'branches':
            branch = path_parts[1]
            repo_path_prefix = f"branches/{branch}"

        else:
            raise SvnExternalError(path_parts)

        if revision:
            branch += f"@{revision}"

        return repo_path_prefix, branch or None

    def get_unlinked_externals(self):
        svn_externals_repos, svn_externals_local_symlinks = self.get_externals()

        submodules_temp_config = self.get_submodules_temp_config()

        external_repos = defaultdict(list)
        svn_url_prefix = os.path.commonprefix([self.svn_url, *[external[0] for external in svn_externals_repos]])
        for external_url, local_path, revision in svn_externals_repos:
            svn_repo_name, repo_path = external_url.removeprefix(svn_url_prefix).split('/', 1)
            external_repos[svn_repo_name].append([repo_path, local_path, revision])

        external_paths_table = Table(
            Column("Local Path"),
            Column("External SVN-Repository"),
            Column("Destination"),
            Column("Branch"),
            Column("Status", justify="right"),
        )
        external_repos_unlinked = defaultdict(list)

        for destination_path, local_path, revision in svn_externals_local_symlinks:
            repo_path_prefix, branch = self.svn_path_to_branch(destination_path, revision)
            external_paths_table.add_row(
                local_path,
                None,
                destination_path.removeprefix(repo_path_prefix).removeprefix('/'),
                branch,
                "✅" if os.path.exists(os.path.join(self.working_dir, local_path)) else "",
            )

        for svn_repo_name, paths in external_repos.items():
            if (
                not paths
                or svn_repo_name in submodules_temp_config
                or next(filter(lambda s: s.get('svn_repo_name') == svn_repo_name, submodules_temp_config.values()), None)
            ):
                continue

            for repo_path_full, local_path, revision in paths:
                is_linked = os.path.exists(os.path.join(self.repo.working_dir, local_path))
                svn_path = os.path.commonprefix([path[0] for path in paths])
                repo_path_prefix, branch = self.svn_path_to_branch(svn_path, revision)
                repo_path = repo_path_full.removeprefix(repo_path_prefix).removeprefix('/')

                external_paths_table.add_row(
                    local_path,
                    svn_repo_name,
                    repo_path,
                    branch,
                    "✅" if is_linked else "",
                )

                if not is_linked:
                    external_repos_unlinked[svn_repo_name].append((repo_path, local_path, branch))

        return external_repos_unlinked, external_paths_table, submodules_temp_config

    def migrate_externals_to_submodules(self, submodules_temp_config: Optional[dict] = None, progress: Optional[Progress] = None):
        files_to_add = set()
        if submodules_temp_config is None:
            submodules_temp_config = self.get_submodules_temp_config()

        for submodule in submodules_temp_config.values():
            do_reset = False
            if 'do_skip' in submodule:
                yield
                continue

            try:
                self.repo.submodule(submodule['repo_name'])

            except ValueError:
                do_reset = True
                shutil.rmtree(os.path.join(self.working_dir, submodule['submodule_path']), onerror=rmtree_error_handler)
                shutil.rmtree(os.path.join(self.working_dir, ".git", "modules", submodule['repo_name']), onerror=rmtree_error_handler)

                progress.log(
                    f"Creating submodule {submodule['repo_name']} ({submodule['git_external_url']}@{submodule['branch'] or 'main'}) at {submodule['submodule_path']}"
                )
                submodule = self.repo.create_submodule(
                    name=submodule['repo_name'],
                    path=submodule['submodule_path'],
                    url=submodule['git_external_url'],
                    branch=submodule['branch'],
                )
                files_to_add.add('.gitmodules')
                if submodule.get('commit'):
                    submodule_repo = submodule.module()
                    submodule_repo.head.reset(submodule.get('commit'), working_tree=True)

            for repo_path, local_path, branch in submodule['paths']:
                full_local_path = os.path.join(self.working_dir, local_path)
                if os.path.exists(full_local_path):
                    if do_reset:
                        os.remove(full_local_path)

                    else:
                        continue

                full_local_path_dirname = os.path.dirname(full_local_path)
                origin = os.path.join(
                    submodule['submodule_path'],
                    submodule['common_path_replacement'] + repo_path.removeprefix(submodule['common_path_prefix']),
                )
                origin_relative = os.path.join(os.path.relpath(self.working_dir, full_local_path_dirname), origin)
                progress.log(f"Creating symlink {local_path} to {origin_relative}")

                os.makedirs(full_local_path_dirname, exist_ok=True)
                os.symlink(
                    origin_relative,
                    full_local_path,
                    target_is_directory=os.path.isdir(os.path.join(self.working_dir, origin)),
                )
                files_to_add.add(os.path.relpath(full_local_path, self.working_dir))

            yield

        if files_to_add:
            self.repo.git.add(*files_to_add)  # repo.index.add does not work with symlinks
            commit = self.repo.index.commit("Add svn externals as git submodules", skip_hooks=True, author=Actor("svn2git", "svn2git@example.com"))
            cherrypick_to_all_branches_with_progress(self.repo, commit, progress, self.refs_to_push)
