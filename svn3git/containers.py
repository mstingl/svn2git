from typing import List, Optional

from pydantic import BaseModel, Field, RootModel


class ExternalPath(BaseModel):
    repo: str
    local: str
    ref: Optional[str]


class SubmoduleConfig(BaseModel):
    repo_name: str = ""
    svn_repo_name: Optional[str]
    submodule_path: str = ""
    git_external_url: Optional[str]
    branch: Optional[str]
    commit: Optional[str]
    common_path_replacement: str = ""
    common_path_prefix: str = ""
    do_skip: bool = False
    paths: List[ExternalPath] = Field(default_factory=list)


class SubmoduleConfigFile(RootModel[dict[str, SubmoduleConfig]]):
    def __getitem__(self, key):
        return self.root[key]

    def __setitem__(self, key, value):
        self.root[key] = value

    def __contains__(self, key):
        return key in self.root

    def __len__(self):
        return len(self.root)

    def values(self):
        return self.root.values()
