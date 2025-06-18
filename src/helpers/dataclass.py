from dataclasses import dataclass, asdict, field
from typing import Dict, Any
import yaml
import json


@dataclass
class Athena:
    type: str = "athena"
    database: str = None
    table: str = None
    query: str = field(default=None, metadata={"optional": True})

@dataclass
class S3:
    type: str = "file"
    bucket: str = None
    prefix: str = None
    file_name: str = field(default=None, metadata={"optional": True})
@dataclass
class Api:
    type: str = "api"
    secrets: str = None
    url : str = None
    
### AWS resources can be aadded above this line if needed ###
    

@dataclass
class Raw:
    athena: Athena = field(default_factory=Athena, metadata={"optional": True})
    s3: S3 = field(default_factory=S3, metadata={"optional": True})
    api: Api = field(default_factory=Api, metadata={"optional": True})
@dataclass
class Trusted:
    athena: Athena = field(default_factory=Athena, metadata={"optional": True})
    s3: S3 = field(default_factory=S3, metadata={"optional": True})
    api: Api = field(default_factory=Api, metadata={"optional": True})
@dataclass
class Enriched:
    athena: Athena = field(default_factory=Athena, metadata={"optional": True})
    s3: S3 = field(default_factory=S3, metadata={"optional": True})
    api: Api = field(default_factory=Api, metadata={"optional": True})
    
@dataclass
class HCO:
    raw: Raw = field(default_factory=Raw)
    trusted: Trusted = field(default_factory=Trusted)
    enriched: Enriched = field(default_factory=Enriched)
    def __getitem__(self, key):
        return getattr(self, key)
    def __iter__(self):
        return iter(self.__dict__)
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
@dataclass
class HCP:
    raw: Raw = field(default_factory=Raw)
    trusted: Trusted = field(default_factory=Trusted)
    enriched: Enriched = field(default_factory=Enriched)
    def __getitem__(self, key):
        return getattr(self, key)
    def __iter__(self):
        return iter(self.__dict__)
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
@dataclass
class DNB:
    hco: HCO = field(default_factory=HCO)
    hcp: HCP = field(default_factory=HCP)
    def __getitem__(self, key):
        return getattr(self, key)
    def __iter__(self):
        return iter(self.__dict__)
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
@dataclass
class SAP:
    hco: HCO = field(default_factory=HCO)
    hcp: HCP = field(default_factory=HCP)
    def __getitem__(self, key):
        return getattr(self, key)
    def __iter__(self):
        return iter(self.__dict__)
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

@dataclass
class Pipeline:
    sap: SAP = field(default_factory=SAP)
    dnb: DNB = field(default_factory=DNB)
    def __getitem__(self, key):
        return getattr(self, key)
    def __iter__(self):
        return iter(self.__dict__)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    


# TODO: The below code needs to be commented out before each code push

def load_pipeline_from_yaml(file_path: str) -> Pipeline:
    with open(file_path, 'r') as yaml_file:
        config_data = yaml.safe_load(yaml_file)
    return Pipeline(**config_data)

# Load the YAML file and display the dictionary
pipeline = load_pipeline_from_yaml('/workspaces/mdm/metadata/config_dev.yaml')
print(pipeline)
# output_path = '/workspaces/mdm/metadata/config_dev.json'
# with open(output_path, 'w') as json_file:
#     json.dump(pipeline.to_dict(), json_file, indent=4)
# pip_dict  = pipeline.to_dict()
# print(pip_dict)
print('#################### WRITE MAPPER TO FIX THE BELOW ISSUE ###################')
print(pipeline.sap)
print('#################### WRITE MAPPER TO FIX THE BELOW ISSUE ###################')
print(pipeline.dnb)

# print(Pipeline)
# Convert pip_dict back into a Pipeline object
# def dict_to_pipeline(data: Dict[str, Any]) -> Pipeline:
#     def recursive_dataclass(cls, data):
#         if isinstance(data, dict):
#             fields = {field.name: field.type for field in cls.__dataclass_fields__.values()}
#             return cls(**{key: recursive_dataclass(fields[key], value) if key in fields else value for key, value in data.items()})
#         return data

#     return recursive_dataclass(Pipeline, data)

# pipeline_object = dict_to_pipeline(pip_dict)
# print(pipeline_object)



