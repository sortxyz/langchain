import os
import time

import pytest

from langchain.document_loaders.sort_blockchain import SortBlockchainDocumentLoader, SortQueryType

from langchain.document_loaders import BlockchainDocumentLoader
from langchain.document_loaders.blockchain import BlockchainType

demo_api_key = "0ab6734b-1e8e-443a-86d7-344a8eb86566"
NUM_RESULTS = 100

def test_nft_metadata() -> None:
    result = SortBlockchainDocumentLoader(
        contract_address="0x887F3909C14DAbd9e9510128cA6cBb448E932d7f", # Chillennials contract address
        api_key=demo_api_key,
        query_type=SortQueryType.NFT_METADATA).load()

    print("Rows returned: ", len(result))

    assert len(result) == NUM_RESULTS, (
        f"Wrong number of rows returned.  "
        f"Expected {NUM_RESULTS}, got {len(NUM_RESULTS)}"
    )

def test_latest_transactions() -> None:
    result = SortBlockchainDocumentLoader(
        contract_address="0x887F3909C14DAbd9e9510128cA6cBb448E932d7f", # Chillennials contract address
        api_key=demo_api_key,
        query_type=SortQueryType.LATEST_TRANSACTIONS).load()

    print("Rows returned: ", len(result))

    assert len(result) == NUM_RESULTS, (
        f"Wrong number of rows returned.  "
        f"Expected {NUM_RESULTS}, got {len(NUM_RESULTS)}"
    )

def test_sql() -> None:
    result = SortBlockchainDocumentLoader(
        sql="select * from ethereum.nft_metadata where contract_address = '0x887f3909c14dabd9e9510128ca6cbb448e932d7f' limit 100",
        api_key=demo_api_key,
        query_type=SortQueryType.SQL).load()

    print("Rows returned: ", len(result))

    assert len(result) == NUM_RESULTS, (
        f"Wrong number of rows returned.  "
        f"Expected {NUM_RESULTS}, got {len(NUM_RESULTS)}"
    )

