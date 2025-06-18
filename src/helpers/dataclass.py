from dataclasses import dataclass, asdict
@dataclass
class Dnb:
    class raw:
        class source:
            type: str = "athena"
            database: str = "adl_enriched_gbl_cf_ebx"
            table:str = "ebx_hierarchy_hierarchy_dunAndBradstreetAccounts"
        class target:
            type: str = "file"
            bucket: str = "adl-base-customer-mdm-etl-dev-226aog"
            prefix: str = "/raw/FS_DNB/"

    def to_dict(self):
        return asdict(self)
