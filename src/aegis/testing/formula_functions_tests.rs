//! Tests for `aegis::formula::functions`.
use crate::aegis::formula::functions_registry::FunctionRegistry;

#[test]
// ROUND redondea PI a 2 decimales; el literal esperado 3.14 es el resultado
// canónico de esa operación, no una aproximación accidental de PI.
#[allow(clippy::approx_constant)]
fn test_standard_functions() {
    let reg = FunctionRegistry::standard();

    // 1. ABS
    let abs_fn = reg.get("ABS").unwrap();
    assert_eq!(abs_fn.evaluate(&[-5.5]), Some(5.5));
    assert_eq!(abs_fn.evaluate(&[3.0]), Some(3.0));

    // 2. ROUND
    let round_fn = reg.get("ROUND").unwrap();
    assert_eq!(round_fn.evaluate(&[std::f64::consts::PI, 2.0]), Some(3.14));
    assert_eq!(round_fn.evaluate(&[std::f64::consts::PI, 0.0]), Some(3.0));

    // 3. CEIL
    let ceil_fn = reg.get("CEIL").unwrap();
    assert_eq!(ceil_fn.evaluate(&[3.1]), Some(4.0));
    assert_eq!(ceil_fn.evaluate(&[-3.1]), Some(-3.0));

    // 4. FLOOR
    let floor_fn = reg.get("FLOOR").unwrap();
    assert_eq!(floor_fn.evaluate(&[3.9]), Some(3.0));
    assert_eq!(floor_fn.evaluate(&[-3.9]), Some(-4.0));

    // 5. POWER
    let power_fn = reg.get("POWER").unwrap();
    assert_eq!(power_fn.evaluate(&[2.0, 3.0]), Some(8.0));
    assert_eq!(power_fn.evaluate(&[9.0, 0.5]), Some(3.0));

    // 6. SQRT
    let sqrt_fn = reg.get("SQRT").unwrap();
    assert_eq!(sqrt_fn.evaluate(&[25.0]), Some(5.0));
    assert_eq!(sqrt_fn.evaluate(&[-1.0]), None); // Domain error

    // 7. LOG (ln)
    let log_fn = reg.get("LOG").unwrap();
    assert_eq!(log_fn.evaluate(&[std::f64::consts::E]), Some(1.0));
    assert_eq!(log_fn.evaluate(&[0.0]), None); // Domain error
    assert_eq!(log_fn.evaluate(&[-5.0]), None); // Domain error

    // 8. LOG10
    let log10_fn = reg.get("LOG10").unwrap();
    assert_eq!(log10_fn.evaluate(&[100.0]), Some(2.0));
    assert_eq!(log10_fn.evaluate(&[0.0]), None);

    // 9. MOD
    let mod_fn = reg.get("MOD").unwrap();
    assert_eq!(mod_fn.evaluate(&[10.0, 3.0]), Some(1.0));
    assert_eq!(mod_fn.evaluate(&[10.0, 0.0]), None); // Division/modulo by zero

    // 10. SIGN
    let sign_fn = reg.get("SIGN").unwrap();
    assert_eq!(sign_fn.evaluate(&[15.5]), Some(1.0));
    assert_eq!(sign_fn.evaluate(&[-0.05]), Some(-1.0));
    assert_eq!(sign_fn.evaluate(&[0.0]), Some(0.0));

    // 11. NULLIF
    let nullif_fn = reg.get("NULLIF").unwrap();
    assert_eq!(nullif_fn.evaluate(&[5.0, 5.0]), None);
    assert_eq!(nullif_fn.evaluate(&[5.0, 10.0]), Some(5.0));

    // 12. COALESCE
    let coalesce_fn = reg.get("COALESCE").unwrap();
    assert_eq!(coalesce_fn.evaluate(&[5.0, 10.0]), Some(5.0));
    assert_eq!(coalesce_fn.evaluate(&[std::f64::NAN, 10.0]), Some(10.0));

    // 13. IF
    let if_fn = reg.get("IF").unwrap();
    assert_eq!(if_fn.evaluate(&[1.0, 42.0, 99.0]), Some(42.0));
    assert_eq!(if_fn.evaluate(&[0.0, 42.0, 99.0]), Some(99.0));

    // 14. GREATEST
    let greatest_fn = reg.get("GREATEST").unwrap();
    assert_eq!(greatest_fn.evaluate(&[1.0, 5.0, 3.0]), Some(5.0));
    assert_eq!(greatest_fn.evaluate(&[]), None);

    // 15. LEAST
    let least_fn = reg.get("LEAST").unwrap();
    assert_eq!(least_fn.evaluate(&[1.0, 5.0, 3.0]), Some(1.0));
    assert_eq!(least_fn.evaluate(&[]), None);

    // 16. CLAMP
    let clamp_fn = reg.get("CLAMP").unwrap();
    assert_eq!(clamp_fn.evaluate(&[50.0, 0.0, 100.0]), Some(50.0));
    assert_eq!(clamp_fn.evaluate(&[-10.0, 0.0, 100.0]), Some(0.0));
    assert_eq!(clamp_fn.evaluate(&[150.0, 0.0, 100.0]), Some(100.0));
}
