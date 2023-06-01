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
        limit: int = 100,
        get_all_tokens: bool = False,
        max_execution_time: Optional[int] = None,
    ):
        self.contract_address = contract_address
        self.blockchainType = blockchainType.value
        self.queryType = queryType.value
        self.api_key = os.environ.get("SORT_API_KEY") or api_key
        self.startToken = startToken
        self.sql = sql
        self.limit = limit
        self.get_all_tokens = get_all_tokens
        self.max_execution_time = max_execution_time

        if not self.api_key:
            raise ValueError("Sort API key not provided.")

        if not re.match(r"^0x[a-fA-F0-9]{40}$", self.contract_address):
            raise ValueError(f"Invalid contract address {self.contract_address}")

    def load(self) -> List[Document]:
        ITEMS_PER_PAGE = 2
        query_offset = 0

        result = []
        print("Loading from Sort...")
        print(self.queryType)
        print(SortQueryType.LATEST_TRANSACTIONS);

        # Default query for latest transactions
        query = "select * from {}.transaction t, ethereum.block b where t.to_address = '{}' and b.id=t.block_id order by b.timestamp desc limit {}".format(self.blockchainType, self.contract_address.lower(), ITEMS_PER_PAGE)

        # SQL query
        if self.queryType == SortQueryType.NFT_METADATA:
            query = "SELECT * FROM {}.nft_metadata WHERE contract_address = '{}' order by token_id desc limit {}".format(self.blockchainType, self.contract_address.lower(), ITEMS_PER_PAGE)
        elif self.queryType == SortQueryType.SQL:
            query = self.sql
            
        # Loop through pages until self.limit, break when at the end
        while True:
            # Send query to API
            print("Loading latest transactions from Sort...")
            print(query)
            url = 'https://api.sort.xyz/v1/queries/run'
            headers = {
                'x-api-key': self.api_key,
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            }
            body = {
                "query": query + " OFFSET " + str(query_offset)
            }

            response = requests.post(url, json = body, headers = headers)
            items = response.json()["data"]["records"]

            if not items:
                break
            
            for item in items:
                content = str(item)
                metadata = {
                    "source": self.contract_address,
                    "blockchain": self.blockchainType
                }
                result.append(Document(page_content=content, metadata=metadata))

            query_offset += ITEMS_PER_PAGE

            if (query_offset >= self.limit):
                break


        return result

   
