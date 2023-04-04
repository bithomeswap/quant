import binascii
import ecdsa
import hashlib
from web3 import Web3
from pymongo import MongoClient

web3 = Web3(Web3.HTTPProvider(
    'https://mainnet.infura.io/v3/f513ad5d3ec343fabb8b1b1e976308ec'))
# 连接MongoDB数据库
client = MongoClient(
    'mongodb://wth000:wth000@43.159.47.250:27017/dbname?authSource=wth000')
db = client['wth000']


def get_address():
    # 遍历查询最近1000个区块
    for block_number in range(web3.eth.block_number-999, web3.eth.block_number+1):
        block = web3.eth.get_block(block_number)
        # 遍历区块中的每条交易记录
        for transaction in block.transactions:
            tx = web3.eth.get_transaction(transaction)
            # 判断交易是否是转账交易，即判断交易数据是否为空并且发送的ETH数量是否大于0。
            if tx.input == '0x' and tx.value > 0:
                # 判断收款地址是否为非多签地址，并且持有的ETH数量是否大于等于1ETH
                if len(web3.eth.get_code(tx.to)) == 2 and tx.value >= web3.toWei(1, 'ether'):
                    data = {'address': tx.to.lower(), 'value': tx.value}
                    # 判断数据是否已经存在于数据库中
                    if db['ETH地址'].count_documents(data) == 0:
                        # 插入数据到MongoDB数据库
                        db['ETH地址'].insert_one(data)


def generate_key():
    # 生成随机的私钥
    private_key = ecdsa.SigningKey.generate(curve=ecdsa.SECP256k1)

    # 获取对应的公钥和以太坊地址
    public_key = binascii.hexlify(
        private_key.get_verifying_key().to_string()).decode()
    sha3_hash = hashlib.sha3_256(
        binascii.unhexlify(public_key[2:])).hexdigest()
    eth_address = '0x' + sha3_hash[-40:]
    data = {'address': eth_address.lower(
    ), 'private_key': private_key.to_string(), 'public_key': public_key}
    # 判断数据是否已经存在于数据库中
    if db['ETH私钥'].count_documents({'address': eth_address.lower()}) == 0:
        # 插入数据到MongoDB数据库
        db['ETH私钥'].insert_one(data)


if __name__ == '__main__':
    get_address()
    generate_key()
