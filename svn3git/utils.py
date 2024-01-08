import os
from contextlib import contextmanager
from typing import Callable, Optional

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
    start: bool = True,
):
    task_id = progress.add_task(name, total=total, start=start)
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
    return [folder for folder in repo.git.svn(f"show-{cmd}", "--id=origin/trunk").splitlines() if folder.strip() and not folder[0] == '#']


def reference_name(reference):
    return reference.remote_head if reference.is_remote() else reference.name


def cherrypick_to_all_branches(
    repo: Repo, commit: Commit, refs_to_push: set, progress: Callable[[dict], None] = lambda d: None, log: Callable[[str], None] = lambda s: None
):
    total = len(repo.branches)
    completed = 0
    for branch in repo.branches:
        if branch.name == "main":
            continue

        branch.checkout()
        try:
            repo.git.cherry_pick(commit.hexsha)

        except GitCommandError:
            repo.git.cherry_pick(skip=True)

        refs_to_push.add(branch)
        completed += 1
        progress({'completed': completed, 'total': total})
        log(f"Cherry-picked {commit.hexsha} to branch {branch.name}")

    repo.branches["main"].checkout()
    refs_to_push.add(repo.branches["main"])
    progress({'completed': total, 'total': total})
