import os
import re
import time
from enum import Enum
from typing import List, Optional

import requests

from langchain.docstore.document import Document
from langchain.document_loaders.base import BaseLoader

class SortBlockchain(str, Enum):
    ETH_MAINNET = "ethereum"
    ETH_GOERLI = "goerli"
    POLYGON_MAINNET = "polygon"

class SortQueryType(str, Enum):
    LATEST_TRANSACTIONS = "latest_transactions"
    NFT_METADATA = "nft-metadata"
    SQL = "SQL"

class SortDocumentLoader(BaseLoader):
    """Add a lot of documentation here
    """

    def __init__(
        self,
        contract_address: str,
        blockchainType: SortBlockchain = SortBlockchain.ETH_MAINNET,
        queryType: SortQueryType = SortQueryType.LATEST_TRANSACTIONS,
        api_key: str = "",
        startToken: str = "",
        sql: str = "",
        get_all_tokens: bool = False,
        max_execution_time: Optional[int] = None,
    ):
        self.contract_address = contract_address
        self.blockchainType = blockchainType.value
        self.queryType = queryType.value
        self.api_key = os.environ.get("SORT_API_KEY") or api_key
        self.startToken = startToken
        self.sql = sql
        self.get_all_tokens = get_all_tokens
        self.max_execution_time = max_execution_time

        if not self.api_key:
            raise ValueError("Sort API key not provided.")

        if not re.match(r"^0x[a-fA-F0-9]{40}$", self.contract_address):
            raise ValueError(f"Invalid contract address {self.contract_address}")

    def load(self) -> List[Document]:
        result = []
        print("Loading from Sort...")
        print(self.queryType)
        print(SortQueryType.LATEST_TRANSACTIONS);

        # Default query for latest transactions
        query = "select * from {}.transaction t, ethereum.block b where t.to_address = '{}' and b.id=t.block_id order by b.timestamp desc limit 100".format(self.blockchainType, self.contract_address.lower())

        # SQL query
        if self.queryType == SortQueryType.NFT_METADATA:
            query = "SELECT * FROM {}.nft_metadata WHERE contract_address = '{}'".format(self.blockchainType, self.contract_address.lower())
        elif self.queryType == SortQueryType.SQL:
            query = self.sql

        # Execute API query
        print("Loading latest transactions from Sort...")
        print(query)

        url = 'https://api.sort.xyz/v1/queries/run'
        headers = {
            'x-api-key': self.api_key,
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        body = {
            "query": query
        }

        response = requests.post(url, json = body, headers = headers)

        items = response.json()["data"]["records"]
        
        for item in items:
            content = str(item)
            metadata = {
                "source": self.contract_address,
                "blockchain": self.blockchainType
            }
            result.append(Document(page_content=content, metadata=metadata))


        return result

   
