//! Feature flag system for Yellowstone Jet
//!
//! This module provides the ability to enable or disable features via configuration,
//! and translate them into the appropriate proto enum values when communicating
//! with the jet-gateway service.
//!
//! The system works by:
//! 1. Reading string-based feature flags from YML configuration
//! 2. Translating them to proto enum values when sending to the gateway
//! 3. Providing utility methods to check if features are enabled

use {
    crate::proto::jet::Feature,
    serde::{de, Deserialize},
    std::collections::HashSet,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FeatureFlag {
    TransactionPayloadV2,
}

impl<'de> Deserialize<'de> for FeatureFlag {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let feature_str = String::deserialize(deserializer)?;
        match feature_str.as_str() {
            "transaction_payload_v2" => Ok(FeatureFlag::TransactionPayloadV2),
            _ => Err(de::Error::custom(format!(
                "Unknown feature: {}",
                feature_str
            ))),
        }
    }
}

impl FeatureFlag {
    const fn to_proto_feature(&self) -> Feature {
        match self {
            FeatureFlag::TransactionPayloadV2 => Feature::TransactionPayloadV2,
        }
    }

    fn from_str(feature_str: impl AsRef<str>) -> Option<Self> {
        match feature_str.as_ref() {
            "transaction_payload_v2" => Some(FeatureFlag::TransactionPayloadV2),
            _ => None,
        }
    }
}

#[derive(Default, Debug, Clone, Deserialize)]
pub struct FeatureSet {
    #[serde(default = "HashSet::new")]
    enabled_features: HashSet<FeatureFlag>,
}

impl FeatureSet {
    pub fn is_enabled(&self, feature_str: impl AsRef<str>) -> bool {
        if let Some(feature) = FeatureFlag::from_str(feature_str) {
            self.enabled_features.contains(&feature)
        } else {
            false
        }
    }

    pub fn is_feature_enabled(&self, feature: Feature) -> bool {
        match feature {
            Feature::TransactionPayloadV2 => self
                .enabled_features
                .contains(&FeatureFlag::TransactionPayloadV2),
            Feature::Unspecified => false,
        }
    }

    pub fn enabled_features(&self) -> Vec<i32> {
        self.enabled_features
            .iter()
            .map(|f| f.to_proto_feature() as i32)
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.enabled_features.is_empty()
    }

    #[cfg(test)]
    pub fn new_with_features(features: &[&str]) -> Self {
        let enabled_features = features
            .iter()
            .filter_map(|f| FeatureFlag::from_str(*f))
            .collect();
        Self { enabled_features }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, serde_yaml::from_str};

    #[test]
    fn test_feature_serialization() {
        let config = r#"
        enabled_features:
          - transaction_payload_v2
        "#;

        let feature_set: FeatureSet = from_str(config).unwrap();
        assert!(feature_set.is_enabled("transaction_payload_v2"));
        assert!(!feature_set.is_enabled("unknown_feature"));

        let empty_config = r#"
        enabled_features: []
        "#;
        let empty_feature_set: FeatureSet = from_str(empty_config).unwrap();
        assert!(!empty_feature_set.is_enabled("transaction_payload_v2"));
    }

    #[test]
    fn test_proto_feature_conversion() {
        let config = r#"
        enabled_features:
          - transaction_payload_v2
        "#;

        let feature_set: FeatureSet = from_str(config).unwrap();
        let proto_features = feature_set.enabled_features();

        assert_eq!(proto_features.len(), 1);
        assert_eq!(proto_features[0], Feature::TransactionPayloadV2 as i32);

        assert!(feature_set.is_feature_enabled(Feature::TransactionPayloadV2));
        assert!(!feature_set.is_feature_enabled(Feature::Unspecified));
    }

    #[test]
    fn test_empty_features() {
        let feature_set = FeatureSet::default();
        assert!(feature_set.is_empty());
        assert!(feature_set.enabled_features().is_empty());
        assert!(!feature_set.is_feature_enabled(Feature::TransactionPayloadV2));
    }

    #[test]
    fn test_new_with_features() {
        let feature_set = FeatureSet::new_with_features(&["transaction_payload_v2"]);
        assert!(!feature_set.is_empty());
        assert_eq!(feature_set.enabled_features().len(), 1);
        assert!(feature_set.is_feature_enabled(Feature::TransactionPayloadV2));
    }

    #[test]
    fn test_unknown_features_ignored() {
        let feature_set =
            FeatureSet::new_with_features(&["transaction_payload_v2", "unknown_feature"]);
        assert_eq!(feature_set.enabled_features().len(), 1);
    }
}
