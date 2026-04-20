from pydantic import AnyHttpUrl, AnyUrl, BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class TiledSettings(BaseModel):
    uri: AnyUrl
    token: str


class OpenMetadataSettings(BaseModel):
    base_url: AnyHttpUrl
    token: str
    catalog_name: str


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_nested_delimiter="__")

    tiled: TiledSettings
    amsc_openmetadata: OpenMetadataSettings
