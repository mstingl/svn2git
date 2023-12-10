import os
from contextlib import contextmanager
from typing import Optional

from git import Commit, Repo
from git.exc import GitCommandError
from rich.progress import Progress


@contextmanager
def task(
    progress: Progress,
    name: str,
    indeterminate: bool = True,
    total: Optional[int] = None,
    remove_after_complete: bool = False,
):
    task_id = progress.add_task(name, total=total)
    yield task_id
    if indeterminate:
        count = total or progress._tasks[task_id].total or 1
        progress.update(task_id, completed=count, total=count)

    if remove_after_complete:
        progress.remove_task(task_id)

    else:
        progress.stop_task(task_id)


def rmtree_error_handler(func, path, exc_info):
    if not os.path.exists(path):
        pass

    elif not os.access(path, os.W_OK):
        import stat

        os.chmod(path, stat.S_IWUSR)
        func(path)

    else:
        raise


def git_progress(
    op_code,
    cur_count,
    max_count,
    message,
    *,
    progress: Progress,
    task_id,
):
    progress.update(
        task_id,
        completed=cur_count,
        total=max_count,
    )


def git_svn_show(repo, cmd) -> list[str]:
    return [
        folder
        for folder in repo.git.svn(f"show-{cmd}", "--id=origin/trunk").splitlines()
        if folder.strip() and not folder[0] == '#'
    ]


def reference_name(reference):
    return reference.remote_head if reference.is_remote() else reference.name


def cherrypick_to_all_branches(repo: Repo, commit: Commit, refs_to_push: set):
    for branch in repo.branches:
        if branch.name == "main":
            continue

        branch.checkout()
        try:
            repo.git.cherry_pick(commit.hexsha)

        except GitCommandError:
            repo.git.cherry_pick(skip=True)

        refs_to_push.add(branch)
        yield branch

    repo.branches["main"].checkout()
    refs_to_push.add(repo.branches["main"])


def cherrypick_to_all_branches_with_progress(repo: Repo, commit: Commit, progress: Progress, refs_to_push: set):
    with task(
        progress, "Apply changes to all branches", total=len(repo.branches), remove_after_complete=True
    ) as task_id:
        for branch in cherrypick_to_all_branches(repo, commit, refs_to_push):
            progress.advance(task_id)
            progress.log(f"Cherry-picked {commit.hexsha} to branch {branch.name}")

        progress.advance(task_id)
