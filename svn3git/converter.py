import json
import os
import re
import shutil
from collections import defaultdict
from functools import cache, partial
from types import NoneType
from typing import Callable, Dict, List, Optional

from git import Head, RemoteReference
from git.cmd import handle_process_output
from git.exc import GitCommandError
from git.repo import Repo
from git.util import Actor
from rich import print
from rich.table import Column, Table
from svn.remote import RemoteClient

from .containers import ExternalPath, SubmoduleConfigFile
from .exceptions import MergeError, MissingAuthorError, MissingBranchError, SvnExternalError, SvnFetchError, SvnOptionsReadError, TagExistsError
from .utils import cherrypick_to_all_branches, git_svn_show, reference_name, rmtree_error_handler


class Converter:
    def __init__(self, repo: Repo, force: bool = False, log: Optional[Callable[[str], None]] = None, skip_trunk: bool = True):
        self.repo = repo
        self.working_dir = self.repo.working_dir
        self.force = force
        self.refs_to_push = set()
        self.log = log or print
        self.skip_trunk = skip_trunk
        self.svn_externals_to_git_config_file = os.path.join(self.working_dir, '.svn_externals_to_git_submodules.json')
        self.svn_url: str = self.repo.config_reader().get_value('svn-remote "svn"', 'url')  # type: ignore

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
                process = self.repo.git.svn("fetch", f"--revision={start}:{stop}", "--ignore-refs=tags\\/https?:", as_process=True)
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

                    self.repo.delete_tag(tag_name)  # type: ignore
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
                        self.log("Updating branch %s from %s to %s" % (branch_name, branch.commit.hexsha, reference.commit.hexsha))
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

    def migrate_gitignore(self, cherrypick_progress: Callable[[dict], None] = lambda d: None):
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
            if self.repo.git.check_ignore(self.working_dir + filepath, no_index=True, with_exceptions=False, with_extended_output=True)[0] != 0  # type: ignore
            and not filepath in current_gitignore
        ]:
            self.log("[cyan]Updating [b].gitignore[/b][/cyan]")
            with open(gitignore_path, 'a') as gitignore:
                gitignore.write(os.linesep + os.linesep.join(["# migrated from SVN"] + ignores_to_add) + os.linesep)

            self.repo.index.add(['.gitignore'])
            commit = self.repo.index.commit("Migrate svn:ignore to .gitignore", skip_hooks=True, author=Actor("svn2git", "svn2git@example.com"))
            cherrypick_to_all_branches(self.repo, commit, self.refs_to_push, cherrypick_progress, log=self.log)

    @cache
    def get_externals(self):
        external_repos: defaultdict[str | NoneType, List[ExternalPath]] = defaultdict(list)
        svn_externals_repos: List[ExternalPath] = []
        external: str
        for external in git_svn_show(self.repo, "externals"):
            if external.endswith('#'):
                continue

            match = re.match(
                r"^(\/(?P<path_base>.*?))?((\/)|(-r \d+ ))((?P<url>(\w+):\/\/[^ @]+)|(\^\/(?P<current>[^ @]+))|(?P<parent>\.\.\/[^ @]+))(@(?P<revision>\d+))? (?P<path_local>.*)$",
                external,
            )
            if not match:
                raise ValueError(external)

            path = (f"{match.group('path_base')}/" if match.group('path_base') else "") + match.group('path_local')

            if match.group('url'):
                svn_externals_repos.append(
                    ExternalPath(
                        repo=match.group('url'),
                        local=path,
                        ref=match.group('revision'),
                    )
                )

            elif match.group('parent'):
                if match.group('revision'):
                    raise ValueError

                external_repos[None].append(
                    ExternalPath(
                        repo=os.path.relpath(os.path.join(os.path.dirname(path), match.group('parent'))),
                        local=path,
                        ref=None,
                    )
                )

            elif match.group('current'):
                destination_path = match.group('current')
                repo_path_prefix, ref = self.svn_path_to_ref(destination_path, match.group('revision'))
                external_repos[None].append(
                    ExternalPath(
                        repo=destination_path.removeprefix(repo_path_prefix).removeprefix('/'),
                        local=path,
                        ref=ref,
                    )
                )

            else:
                raise NotImplementedError

        svn_url_prefix = os.path.commonprefix([self.svn_url, *[path.repo for path in svn_externals_repos]])
        for path in svn_externals_repos:
            svn_repo_name, repo_path = path.repo.removeprefix(svn_url_prefix).split('/', 1)
            repo_path_prefix, ref = self.svn_path_to_ref(repo_path, path.ref)
            external_repos[svn_repo_name].append(
                ExternalPath(
                    repo=repo_path.removeprefix(repo_path_prefix).removeprefix('/'),
                    local=path.local,
                    ref=ref,
                )
            )

        return external_repos

    def get_submodules_temp_config(self) -> SubmoduleConfigFile:
        try:
            with open(self.svn_externals_to_git_config_file, 'rb') as config_file:
                return SubmoduleConfigFile.model_validate_json(str(config_file.read(), 'utf-8') or r'{}')

        except (FileNotFoundError, json.JSONDecodeError):
            return SubmoduleConfigFile(root={})

    def svn_path_to_ref(self, svn_path, revision):
        path_parts = [p for p in svn_path.strip('/').split('/') if p]
        if not path_parts:
            raise ValueError

        elif path_parts[0] == "trunk":
            ref = ""  # default / main
            repo_path_prefix = "trunk"

        elif path_parts[0] == 'branches':
            ref = path_parts[1]
            repo_path_prefix = f"branches/{ref}"

        elif path_parts[0] == 'tags':
            ref = path_parts[1]
            repo_path_prefix = f"tags/{ref}"

        else:
            raise SvnExternalError(path_parts)

        if revision and path_parts[0] != 'tags':
            ref += f"@{revision}"

        return repo_path_prefix, ref or None

    def get_unlinked_externals(self):
        external_repos = self.get_externals()
        submodules_temp_config = self.get_submodules_temp_config()

        external_paths_table = Table(
            Column("Local Path"),
            Column("External SVN-Repository"),
            Column("Destination"),
            Column("Branch / Ref"),
            Column("Status", justify="right"),
        )
        external_repos_unlinked: defaultdict[str | NoneType, List[ExternalPath]] = defaultdict(list)

        for svn_repo_name, paths in external_repos.items():
            if not paths:
                continue

            is_submodule_defined = svn_repo_name and (
                svn_repo_name in submodules_temp_config
                or next(filter(lambda s: s.svn_repo_name == svn_repo_name, submodules_temp_config.values()), None)
            )

            for path in paths:
                is_linked = os.path.exists(os.path.join(self.repo.working_dir, path.local))

                external_paths_table.add_row(
                    path.local,
                    svn_repo_name,
                    path.repo,
                    path.ref,
                    "[green][b]linked[/b][/green]" if is_linked else "",
                )

                if not is_linked and not is_submodule_defined:
                    external_repos_unlinked[svn_repo_name].append(path)

        return dict(external_repos_unlinked), external_paths_table, submodules_temp_config

    def _migrate_local_externals_to_symlinks(self, external_repos_unlinked: Optional[Dict[str | NoneType, List[ExternalPath]]] = None):
        files_to_add = set()
        if external_repos_unlinked is None:
            external_repos_unlinked = self.get_unlinked_externals()[0]

        if not None in external_repos_unlinked:
            return files_to_add

        for path in external_repos_unlinked[None]:
            if path.ref:
                continue

            files_to_add.add(*self._create_symlink(path.local, path.repo))

        return files_to_add

    def migrate_externals_to_submodules(
        self,
        submodules_temp_config: Optional[SubmoduleConfigFile] = None,
        cherrypick_progress: Callable[[dict], None] = lambda d: None,
        external_repos_unlinked: Optional[dict] = None,
    ):
        files_to_add = self._migrate_local_externals_to_symlinks(external_repos_unlinked)
        if submodules_temp_config is None:
            submodules_temp_config = self.get_submodules_temp_config()

        for submodule_config in submodules_temp_config.values():
            do_reset = False
            if submodule_config.do_skip:
                yield
                continue

            try:
                self.repo.submodule(submodule_config.repo_name)

            except ValueError:
                do_reset = True
                shutil.rmtree(os.path.join(self.working_dir, submodule_config.submodule_path), onerror=rmtree_error_handler)
                shutil.rmtree(os.path.join(self.working_dir, ".git", "modules", submodule_config.repo_name), onerror=rmtree_error_handler)

                self.log(
                    f"Creating submodule {submodule_config.repo_name} ({submodule_config.git_external_url}@{submodule_config.branch or 'main'}) at {submodule_config.submodule_path}"
                )
                created_submodule = self.repo.create_submodule(
                    name=submodule_config.repo_name,
                    path=submodule_config.submodule_path,
                    url=submodule_config.git_external_url,
                    branch=submodule_config.branch,
                )
                files_to_add.add('.gitmodules')
                if submodule_config.commit:
                    submodule_repo = created_submodule.module()
                    submodule_repo.head.reset(submodule_config.commit, working_tree=True)

            for path in submodule_config.paths:
                origin = os.path.join(
                    submodule_config.submodule_path,
                    submodule_config.common_path_replacement + path.repo.removeprefix(submodule_config.common_path_prefix).removeprefix('/'),
                )
                files_to_add.add(*self._create_symlink(path.local, origin, do_reset=do_reset))

            yield

        if files_to_add:
            self.repo.git.add(*files_to_add)  # repo.index.add does not work with symlinks
            commit = self.repo.index.commit("Add svn externals as git submodules", skip_hooks=True, author=Actor("svn2git", "svn2git@example.com"))
            cherrypick_to_all_branches(self.repo, commit, self.refs_to_push, cherrypick_progress, log=self.log)

    def _create_symlink(self, local_path: str, origin: str, do_reset: bool = False):
        full_local_path = os.path.join(self.working_dir, local_path)
        if os.path.exists(full_local_path):
            if do_reset:
                os.remove(full_local_path)

            else:
                return

        full_local_path_dirname = os.path.dirname(full_local_path)
        origin_relative = os.path.join(os.path.relpath(self.working_dir, full_local_path_dirname), origin)
        self.log(f"Creating symlink {local_path} to {origin_relative}")

        os.makedirs(full_local_path_dirname, exist_ok=True)
        os.symlink(
            origin_relative,
            full_local_path,
            target_is_directory=os.path.isdir(os.path.join(self.working_dir, origin)),
        )

        yield os.path.relpath(full_local_path, self.working_dir)
