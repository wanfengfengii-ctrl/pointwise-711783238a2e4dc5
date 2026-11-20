"""Run this project's full unittest suite from the repository root."""
import sys
import unittest

import exclusive_case


def main():
    suite = unittest.defaultTestLoader.discover('tests', top_level_dir='.')
    if suite.countTestCases():
        result = unittest.TextTestRunner(verbosity=2).run(suite)
    else:
        print('No tests discovered in tests/', file=sys.stderr)
        result = unittest.TestResult()
    for status, failed_cases in (
        ('FAIL', (test for test, _ in result.failures)),
        ('ERROR', (test for test, _ in result.errors)),
        ('UNEXPECTED_SUCCESS', result.unexpectedSuccesses),
    ):
        for test in failed_cases:
            print(f'{status} {test.id()}')
    suite_ok = result.testsRun > 0 and result.wasSuccessful()
    scenarios_ok = True
    for scenario in ('v1', 'sign', 'withdraw', 'commit'):
        try:
            exclusive_case.run_scenario(scenario)
        except Exception as error:  # noqa: BLE001 - report and fail the verifier
            scenarios_ok = False
            print(f'SCENARIO FAIL {scenario}: {error!r}')
    print(f'TEST SUMMARY total={result.testsRun} failures={len(result.failures)} '
          f'errors={len(result.errors)} skipped={len(result.skipped)} '
          f'expected_failures={len(result.expectedFailures)} '
          f'unexpected_successes={len(result.unexpectedSuccesses)} '
          f'scenarios={"PASS" if scenarios_ok else "FAIL"} '
          f'result={"PASS" if suite_ok and scenarios_ok else "FAIL"}')
    return 0 if suite_ok and scenarios_ok else 1


if __name__ == '__main__':
    raise SystemExit(main())
