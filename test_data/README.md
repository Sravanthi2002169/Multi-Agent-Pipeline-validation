# Golden Test Data (SEN-560)

This folder contains golden datasets for validating multi-agent pipeline outputs.

## Structure

- discovery/
- impact/
- remediation/

Each test case includes:
- Input JSON
- Expected result (PASS / FAIL)

## Coverage

Includes:
- Happy path scenarios
- Edge cases
- Schema validation failures
- Business rule failures
- Partial success cases

## Usage

These datasets are used for:
- Validation testing
- Regression testing (SEN-561)
- Deployment gates (SEN-563)