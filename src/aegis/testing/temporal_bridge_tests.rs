use crate::aegis::temporal_bridge::*;
use crate::grpc::pb::TimeFrameContext;

#[test]
fn proto_custom_range_converts_ms_to_s() {
    let tf = TimeFrameContext {
        r#type: 1,                   // CUSTOM_RANGE
        start_ts: 1_600_000_000_000, // ms
        end_ts: 1_700_000_000_000,   // ms
        n_value: 0,
        timezone: "UTC".to_string(),
    };
    let r = resolve_proto_time_frame(&tf).unwrap();
    assert_eq!(r.start_ts.unwrap(), 1_600_000_000); // s
    assert_eq!(r.end_ts.unwrap(), 1_700_000_000);
}

#[test]
fn proto_all_time_returns_none_bounds() {
    let tf = TimeFrameContext {
        r#type: 29, // ALL_TIME
        ..Default::default()
    };
    let r = resolve_proto_time_frame(&tf).unwrap();
    assert!(r.start_ts.is_none());
    assert!(r.end_ts.is_none());
}

#[test]
fn proto_unspecified_returns_none() {
    let tf = TimeFrameContext {
        r#type: 0, // UNSPECIFIED
        ..Default::default()
    };
    assert!(resolve_proto_time_frame(&tf).is_none());
}
