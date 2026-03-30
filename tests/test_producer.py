import pytest
import uuid
from datetime import datetime
from unittest.mock import patch, MagicMock
import pandas as pd
import sys
import os

# Allow importing from src/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Patch boto3 at import time so producer.py doesn't need real AWS credentials
with patch('boto3.client'):
    from src.producer import enrich_record, load_paysim_data


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_row():
    """A representative PaySim row matching the CSV schema."""
    return {
        'step': 10,
        'type': 'TRANSFER',
        'amount': 1234.5678,
        'nameOrig': 'C123456789',
        'oldbalanceOrg': 5000.00,
        'newbalanceOrig': 3765.43,
        'nameDest': 'C987654321',
        'oldbalanceDest': 0.00,
        'newbalanceDest': 1234.57,
        'isFraud': 0,
        'isFlaggedFraud': 0,
    }


@pytest.fixture
def fraud_row():
    """A PaySim row with fraud flags set."""
    return {
        'step': 743,
        'type': 'CASH_OUT',
        'amount': 250000.00,
        'nameOrig': 'C111111111',
        'oldbalanceOrg': 250000.00,
        'newbalanceOrig': 0.00,
        'nameDest': 'C999999999',
        'oldbalanceDest': 0.00,
        'newbalanceDest': 250000.00,
        'isFraud': 1,
        'isFlaggedFraud': 1,
    }


# ── enrich_record tests ────────────────────────────────────────────────────────

class TestEnrichRecord:

    def test_returns_all_required_fields(self, sample_row):
        result = enrich_record(sample_row)
        expected_keys = [
            'transaction_id', 'step', 'type', 'amount',
            'name_orig', 'old_balance_orig', 'new_balance_orig',
            'name_dest', 'old_balance_dest', 'new_balance_dest',
            'is_fraud', 'is_flagged_fraud', 'timestamp', 'ingested_at'
        ]
        for key in expected_keys:
            assert key in result, f"Missing field: {key}"

    def test_transaction_id_is_valid_uuid(self, sample_row):
        result = enrich_record(sample_row)
        # Should not raise ValueError
        parsed = uuid.UUID(result['transaction_id'])
        assert str(parsed) == result['transaction_id']

    def test_transaction_ids_are_unique(self, sample_row):
        ids = {enrich_record(sample_row)['transaction_id'] for _ in range(100)}
        assert len(ids) == 100, "transaction_id values are not unique"

    def test_amount_rounded_to_two_decimal_places(self, sample_row):
        result = enrich_record(sample_row)
        assert result['amount'] == round(sample_row['amount'], 2)

    def test_timestamp_derived_from_step(self, sample_row):
        result = enrich_record(sample_row)
        expected = datetime(2024, 1, 1, 10, 0, 0).isoformat()  # step=10 hours
        assert result['timestamp'] == expected

    def test_timestamp_step_zero_is_base_date(self, sample_row):
        sample_row['step'] = 0
        result = enrich_record(sample_row)
        assert result['timestamp'] == datetime(2024, 1, 1, 0, 0, 0).isoformat()

    def test_ingested_at_is_recent_iso_timestamp(self, sample_row):
        before = datetime.now()
        result = enrich_record(sample_row)
        after = datetime.now()
        ingested = datetime.fromisoformat(result['ingested_at'])
        assert before <= ingested <= after

    def test_is_fraud_cast_to_int(self, sample_row):
        result = enrich_record(sample_row)
        assert isinstance(result['is_fraud'], int)
        assert result['is_fraud'] == 0

    def test_fraud_flags_preserved(self, fraud_row):
        result = enrich_record(fraud_row)
        assert result['is_fraud'] == 1
        assert result['is_flagged_fraud'] == 1

    def test_type_preserved(self, sample_row):
        result = enrich_record(sample_row)
        assert result['type'] == 'TRANSFER'

    def test_name_fields_mapped_correctly(self, sample_row):
        result = enrich_record(sample_row)
        assert result['name_orig'] == sample_row['nameOrig']
        assert result['name_dest'] == sample_row['nameDest']

    def test_balances_rounded_to_two_decimal_places(self, sample_row):
        result = enrich_record(sample_row)
        assert result['old_balance_orig'] == round(sample_row['oldbalanceOrg'], 2)
        assert result['new_balance_orig'] == round(sample_row['newbalanceOrig'], 2)


# ── load_paysim_data tests ─────────────────────────────────────────────────────

class TestLoadPaySimData:

    def test_returns_list_of_dicts(self, tmp_path):
        csv = tmp_path / "paysim1.csv"
        csv.write_text(
            "step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,"
            "nameDest,oldbalanceDest,newbalanceDest,isFraud,isFlaggedFraud\n"
            "1,PAYMENT,100.0,C1,500.0,400.0,M1,0.0,0.0,0,0\n"
            "2,TRANSFER,200.0,C2,300.0,100.0,C3,0.0,200.0,0,0\n"
        )
        with patch('src.producer.PAYSIM_FILE', str(csv)):
            records = load_paysim_data()
        assert isinstance(records, list)
        assert len(records) == 2
        assert isinstance(records[0], dict)

    def test_raises_on_missing_file(self):
        with patch('src.producer.PAYSIM_FILE', 'nonexistent.csv'):
            with pytest.raises(FileNotFoundError):
                load_paysim_data()
