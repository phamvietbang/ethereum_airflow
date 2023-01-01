CTOKEN_EVENT=[
    {
        "anonymous": False,
        "inputs": [
            {
                "indexed": False,
                "internalType": "address",
                "name": "borrower",
                "type": "address"
            },
            {
                "indexed": False,
                "internalType": "uint256",
                "name": "borrowAmount",
                "type": "uint256"
            },
            {
                "indexed": False,
                "internalType": "uint256",
                "name": "accountBorrows",
                "type": "uint256"
            },
            {
                "indexed": False,
                "internalType": "uint256",
                "name": "totalBorrows",
                "type": "uint256"
            }
        ],
        "name": "Borrow",
        "type": "event"
    },
    {
        "anonymous": False,
        "inputs": [
            {
                "indexed": False,
                "internalType": "address",
                "name": "liquidator",
                "type": "address"
            },
            {
                "indexed": False,
                "internalType": "address",
                "name": "borrower",
                "type": "address"
            },
            {
                "indexed": False,
                "internalType": "uint256",
                "name": "repayAmount",
                "type": "uint256"
            },
            {
                "indexed": False,
                "internalType": "address",
                "name": "vTokenCollateral",
                "type": "address"
            },
            {
                "indexed": False,
                "internalType": "uint256",
                "name": "seizeTokens",
                "type": "uint256"
            }
        ],
        "name": "LiquidateBorrow",
        "type": "event"
    },
    {
        "anonymous": False,
        "inputs": [
            {
                "indexed": False,
                "internalType": "address",
                "name": "minter",
                "type": "address"
            },
            {
                "indexed": False,
                "internalType": "uint256",
                "name": "mintAmount",
                "type": "uint256"
            },
            {
                "indexed": False,
                "internalType": "uint256",
                "name": "mintTokens",
                "type": "uint256"
            }
        ],
        "name": "Mint",
        "type": "event"
    },
    {
        "anonymous": False,
        "inputs": [
            {
                "indexed": False,
                "internalType": "address",
                "name": "redeemer",
                "type": "address"
            },
            {
                "indexed": False,
                "internalType": "uint256",
                "name": "redeemAmount",
                "type": "uint256"
            },
            {
                "indexed": False,
                "internalType": "uint256",
                "name": "redeemTokens",
                "type": "uint256"
            }
        ],
        "name": "Redeem",
        "type": "event"
    },
    {
        "anonymous": False,
        "inputs": [
            {
                "indexed": False,
                "internalType": "address",
                "name": "payer",
                "type": "address"
            },
            {
                "indexed": False,
                "internalType": "address",
                "name": "borrower",
                "type": "address"
            },
            {
                "indexed": False,
                "internalType": "uint256",
                "name": "repayAmount",
                "type": "uint256"
            },
            {
                "indexed": False,
                "internalType": "uint256",
                "name": "accountBorrows",
                "type": "uint256"
            },
            {
                "indexed": False,
                "internalType": "uint256",
                "name": "totalBorrows",
                "type": "uint256"
            }
        ],
        "name": "RepayBorrow",
        "type": "event"
    },
    {
        "anonymous": False,
        "inputs": [
            {
                "indexed": True,
                "internalType": "address",
                "name": "from",
                "type": "address"
            },
            {
                "indexed": True,
                "internalType": "address",
                "name": "to",
                "type": "address"
            },
            {
                "indexed": False,
                "internalType": "uint256",
                "name": "amount",
                "type": "uint256"
            }
        ],
        "name": "Transfer",
        "type": "event"
    }
]