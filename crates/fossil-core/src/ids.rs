use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IdError {
    #[error("identifier is empty or too long")]
    InvalidLength,
    #[error("identifier contains unsupported characters")]
    InvalidCharacters,
    #[error("run ID must be an RFC UUIDv7")]
    RunIdMustBeUuidV7,
    #[error("identifier is not a valid UUID")]
    InvalidUuid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct RunId(Uuid);

impl RunId {
    pub fn parse(value: &str) -> Result<Self, IdError> {
        let uuid = Uuid::parse_str(value).map_err(|_| IdError::InvalidUuid)?;
        if uuid.to_string() != value {
            return Err(IdError::InvalidUuid);
        }
        if uuid.get_version_num() != 7 || uuid.get_variant() != uuid::Variant::RFC4122 {
            return Err(IdError::RunIdMustBeUuidV7);
        }
        Ok(Self(uuid))
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<'de> Deserialize<'de> for RunId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct WorkerId(String);

impl WorkerId {
    pub fn parse(value: impl Into<String>) -> Result<Self, IdError> {
        let value = value.into();
        if value.is_empty() || value.len() > 64 {
            return Err(IdError::InvalidLength);
        }
        if !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(IdError::InvalidCharacters);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<'de> Deserialize<'de> for WorkerId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

macro_rules! opaque_id {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn parse(value: impl Into<String>) -> Result<Self, IdError> {
                let value = value.into();
                if value.is_empty() || value.len() > 512 {
                    return Err(IdError::InvalidLength);
                }
                if value.chars().any(char::is_control) {
                    return Err(IdError::InvalidCharacters);
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::parse(value).map_err(serde::de::Error::custom)
            }
        }
    };
}

opaque_id!(DisplayId);
opaque_id!(QueueMessageId);
opaque_id!(QueueLeaseId);

macro_rules! uuid_id {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            pub fn parse(value: &str) -> Result<Self, IdError> {
                Uuid::parse_str(value)
                    .map(Self)
                    .map_err(|_| IdError::InvalidUuid)
            }

            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }

            pub fn as_str(&self) -> String {
                self.0.to_string()
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

uuid_id!(ClaimCallbackId);
uuid_id!(EventId);
