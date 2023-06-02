import os
import re
import time
from enum import Enum
from typing import List, Optional

import requests

from langchain.docstore.document import Document
from langchain.document_loaders.base import BaseLoader

class SortBlockchainName(str, Enum):
    ETH_MAINNET = "ethereum"
    ETH_GOERLI = "goerli"
    POLYGON_MAINNET = "polygon"

class SortQueryType(str, Enum):
    LATEST_TRANSACTIONS = "latest-transactions"
    NFT_METADATA = "nft-metadata"
    SQL = "SQL"

class SortBlockchainDocumentLoader(BaseLoader):
    """Add a lot of documentation here
    """

    def __init__(
        self,
        contract_address: str = "",
        blockchain_name: SortBlockchainName = SortBlockchainName.ETH_MAINNET,
        query_type: SortQueryType = SortQueryType.LATEST_TRANSACTIONS,
        api_key: str = "",
        sql: str = "",
        limit: int = 100
    ):
        self.contract_address = contract_address
        self.blockchain_name = blockchain_name.value
        self.query_type = query_type.value
        self.api_key = os.environ.get("SORT_API_KEY") or api_key
        self.sql = sql
        self.limit = limit
        
        if not self.api_key:
            raise ValueError("Sort API key is required.")

        if not re.match(r"^0x[a-fA-F0-9]{40}$", self.contract_address) and self.query_type != SortQueryType.SQL:
            raise ValueError(f"Invalid contract address {self.contract_address}")

    def load(self) -> List[Document]:
        ITEMS_PER_PAGE = 100
        query_offset = 0

        result = []
            
        # Loop through pages until self.limit, break when at the end
        while True:
            # Default query for latest transactions
            query = "select * from {}.transaction t, ethereum.block b where t.to_address = '{}' and b.id=t.block_id order by b.timestamp desc limit {} offset {}".format(self.blockchain_name, self.contract_address.lower(), ITEMS_PER_PAGE, query_offset)

            # SQL query
            if self.query_type == SortQueryType.NFT_METADATA:
                query = "SELECT * FROM {}.nft_metadata WHERE contract_address = '{}' order by token_id desc limit {} offset {}".format(self.blockchain_name, self.contract_address.lower(), ITEMS_PER_PAGE, query_offset)
            elif self.query_type == SortQueryType.SQL:
                query = self.sql

            # Send query to API
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

            if not items:
                break
            
            for item in items:
                content = str(item)
                metadata = {
                    "source": self.contract_address,
                    "blockchain": self.blockchain_name
                }
                result.append(Document(page_content=content, metadata=metadata))

            query_offset += ITEMS_PER_PAGE

            if (query_offset >= self.limit or self.query_type == SortQueryType.SQL):
                break


        return result

   
