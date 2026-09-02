use super::*;
use crate::domain::errors::ErrorCode;

#[test]
fn run_executes_all_steps_on_success() {
    let steps: Vec<Step<i32>> = vec![
        Box::new(|n| Ok(n + 1)),
        Box::new(|n| Ok(n * 2)),
        Box::new(|n| Ok(n + 10)),
    ];
    assert_eq!(run(&steps, 5), Ok(22)); // (5+1)*2+10 = 22
}

#[test]
fn run_short_circuits_on_first_error() {
    let steps: Vec<Step<i32>> = vec![
        Box::new(|n| Ok(n + 1)),
        Box::new(|_| Err(DomainError::eav(ErrorCode::Eav001, "paso 2 falló"))),
        Box::new(|n| Ok(n + 100)), // nunca se ejecuta
    ];
    assert!(run(&steps, 5).is_err());
}
