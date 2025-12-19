from pydantic import BaseModel



class DataverseInfo(BaseModel):
    """
    Schema for Dataverse information.
    """
    url: str
    collection_name: str
    api_key: str


class HoustonIngestFlowInput(BaseModel):
    """
    Input schema for the Houston Ingest Flow.
    """
    bucket_name: str
    template_key: str
    mapping_key: str
    issue_key: str
    newspaper_key: str
    dataverse_info: DataverseInfo