use serde::Deserialize;
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Feature {
    PayloadV2,
    BlocklistPdas,
}

impl Feature {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::PayloadV2 => "payload_v2",
            Self::BlocklistPdas => "blocklist_pdas",
        }
    }

    pub fn all() -> impl Iterator<Item = Feature> {
        [Self::PayloadV2, Self::BlocklistPdas].into_iter()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct FeatureSet {
    #[serde(default = "HashSet::new")]
    enabled_features: HashSet<String>,
}

impl Default for FeatureSet {
    fn default() -> Self {
        Self {
            enabled_features: HashSet::new(),
        }
    }
}

impl FeatureSet {
    pub fn is_enabled(&self, feature: Feature) -> bool {
        self.enabled_features.contains(feature.as_str())
    }

    pub fn enabled_features(&self) -> Vec<String> {
        Feature::all()
            .filter(|f| self.is_enabled(*f))
            .map(|f| f.as_str().to_string())
            .collect()
    }

    #[cfg(test)]
    pub fn new_with_features(features: &[Feature]) -> Self {
        let enabled_features = features.iter().map(|f| f.as_str().to_string()).collect();
        Self { enabled_features }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml::from_str;

    #[test]
    fn test_feature_serialization() {
        let config = r#"
        enabled_features:
          - payload_v2
          - blocklist_pdas
        "#;

        let feature_set: FeatureSet = from_str(config).unwrap();
        assert!(feature_set.is_enabled(Feature::PayloadV2));
        assert!(feature_set.is_enabled(Feature::BlocklistPdas));

        let empty_config = r#"
        enabled_features: []
        "#;
        let empty_feature_set: FeatureSet = from_str(empty_config).unwrap();
        assert!(!empty_feature_set.is_enabled(Feature::PayloadV2));
    }

    #[test]
    fn test_enabled_features_list() {
        let config = r#"
        enabled_features:
          - payload_v2
        "#;

        let feature_set: FeatureSet = from_str(config).unwrap();
        let enabled = feature_set.enabled_features();
        assert_eq!(enabled.len(), 1);
        assert!(enabled.contains(&"payload_v2".to_string()));
    }

    #[test]
    fn test_new_with_features() {
        let features = [Feature::PayloadV2, Feature::BlocklistPdas];
        let feature_set = FeatureSet::new_with_features(&features);

        assert!(feature_set.is_enabled(Feature::PayloadV2));
        assert!(feature_set.is_enabled(Feature::BlocklistPdas));
    }
}
