import snorkel

from snorkel.labeling.lf import labeling_function
from snorkel.labeling import PandasLFApplier
from snorkel.labeling.model import LabelModel


def lambda_handler(event, context):

    df = wr.s3.read_csv(path="s3://individualtwitter/preprocessed_data.csv")


    # create spam or not spam labels
    SPAM = 1
    NOT_SPAM = 0
    ABSTAIN = -1

    # create spam labelling functions


    # label as SPAM if there is a link in the tweet

    @labeling_function()
    def lf_contains_link(x):
        return SPAM if "http" in x.text else ABSTAIN

    # create list of words which often occur in spam tweets

    spam_words = ["free", "check", "giveaway", "gift", "present", "subscribe", "follow", "retweet", "luck", 'win']


    # label twet as spam if spam word in tweet

    @labeling_function()
    def lf_contains_words(x):
        return SPAM if any(i in x.text for i in spam_words) else ABSTAIN


    # webscraped a list of crypto jargon from a website. See file webscraping_crypto_jargon for process

    not_spam_words = ['10x',
                      '51% attack',
                      'address',
                      'addy',
                      'algorithm',
                      'altcoin',
                      'a long position',
                      'a short position',
                      'alt markets',
                      'airdrop',
                      'aml',
                      'angel investor',
                      'application cryptocurrencies',
                      'arbitrage',
                      'arbitrage trading',
                      'ash-draked',
                      'asic',
                      'asic miner',
                      'ath',
                      'atl',
                      'augur',
                      'autonomous decentralized organization',
                      'bagholder',
                      'bears',
                      'bearish',
                      'bearishness',
                      'bear market',
                      'bear trap',
                      'best blockchain',
                      'bit',
                      'block',
                      'blockchain',
                      'block explorer',
                      'block header',
                      'block height',
                      'block reward',
                      'bitcoin protocol',
                      'bitcoin network',
                      'bitcoin-js',
                      'bitcoinqt',
                      'bitcoin days destroyed',
                      'bollinger band',
                      'brain wallet',
                      'btc next rsistance',
                      'btc next support',
                      'btc &amp; xbt',
                      'btd',
                      'btfd',
                      'bugling',
                      'bull trap',
                      'bullish',
                      'bulls',
                      'bullishness',
                      'bull market',
                      'buy area',
                      'buy below',
                      'buy wall',
                      'candle',
                      'choyna',
                      'colored corners',
                      'central ledger',
                      'confirmation',
                      'consensus',
                      'consensus rule',
                      'crypto bubble',
                      'cryptocurrency',
                      'cryptography',
                      'cryptojacking',
                      'cve-2012–2459',
                      'dapp',
                      'dao',
                      'darksend',
                      'darkweb',
                      'data size',
                      'deaf at bounce',
                      'decentralized',
                      'deflation',
                      'demurrage',
                      'desktop wallet',
                      'deterministic wallet',
                      'devcon of ethereum',
                      'difficulty',
                      'dildo',
                      'distributed ledger',
                      'double spending',
                      'dust transactions',
                      'dyor',
                      'eli5',
                      'i am',
                      'ether',
                      'ethereum',
                      'etf',
                      'escrow',
                      'exchange',
                      'fa',
                      'faucet',
                      'fiat money',
                      'following scalp',
                      'fomo',
                      'fork',
                      'block height',
                      'frictionless',
                      'fud',
                      'fud',
                      'fudster',
                      'full node?',
                      'fungible',
                      'genesis blocks',
                      'gpu',
                      'halving',
                      'hard cap',
                      'hard fork',
                      'hardware wallet',
                      'hash',
                      'hash rate',
                      'hodl',
                      'ico',
                      'increase leverage',
                      'inflation',
                      'inputs',
                      'jomo',
                      'kimoto gravity well',
                      'kyc',
                      'lambo',
                      'laundry',
                      'leverage',
                      'limit order',
                      'litecoin',
                      'liquidity',
                      'liquidity swap',
                      'macd',
                      'market cap/mcap',
                      'margin trading',
                      'market order',
                      'merged mining',
                      'micro-transaction',
                      'mbtc',
                      'miner',
                      'mining',
                      'mining algorithm',
                      'mining contract',
                      'mining pool',
                      'mining rig',
                      'minting',
                      'mixing service',
                      'mobil wallet',
                      'mooning',
                      'money laundering',
                      'mtgox',
                      'miltisig',
                      'namecoin',
                      'network effect',
                      'nfc',
                      'node',
                      'nonse',
                      'observe the candle',
                      'off blockchain transactions',
                      'orphanded block',
                      'open source',
                      'otc exchange',
                      'output',
                      'output index',
                      'otc',
                      'p2p',
                      'p2pkh',
                      'paper wallet',
                      'peer?',
                      'plasma',
                      'platform cryptocurrencies',
                      'pre-mining',
                      'pre-mining coins',
                      'price bubble',
                      'privacy',
                      'private key',
                      'proof of burn',
                      'proof of existence',
                      'proof of stake',
                      'pow',
                      'proof of work',
                      'public key',
                      'pump and dump',
                      'pubkey',
                      'quantitative easing',
                      'qr code',
                      'recurrent rebilling',
                      'recovery phrase ',
                      'refund',
                      'rekt',
                      'remittance',
                      'response time',
                      'result',
                      'reverse indicator',
                      'ripple',
                      'roi',
                      'rsi',
                      'satoshi',
                      'satoshi nakamoto',
                      'scalability',
                      'scamcoin',
                      'scrypts pubkey',
                      'scrypt',
                      'seed',
                      'self executing contract',
                      'segregated witness soft fork',
                      'segwit (segregated witness)',
                      'sharding',
                      'sidechain',
                      'signature',
                      'signature script',
                      'sigscript',
                      'silk road',
                      'smart contract',
                      'speculator',
                      'spv',
                      'shill',
                      'shitcoin ',
                      'soft cap',
                      'soft fork',
                      'software wallet',
                      'solidity',
                      'stale blocks',
                      'stale block',
                      'stable coin',
                      'surge',
                      'swing',
                      'ta',
                      'taint',
                      'tank',
                      'tcp/ip',
                      'testnet',
                      'the markel tree',
                      'timestamp',
                      'tokens',
                      'tor',
                      'total coin supply',
                      'to the moon',
                      'transaction block',
                      'transactional cryptocurrencies',
                      'transaction fee',
                      'txids',
                      'uri',
                      'utox',
                      'utility cryptocurrencies',
                      'vanity address',
                      'vapourware',
                      'validation rules',
                      'velocity of money',
                      'venture capitalist',
                      'virgin bitcoin',
                      'volatility',
                      'top crypto tool:',
                      'automate trading',
                      'hopper trade bot',
                      'trusted crypto wallet',
                      'super ledger',
                      'wallet',
                      'whale',
                      'whitepaper',
                      'wire transfer',
                      'zerocoin',
                      'zero confirmation transaction']

    # labelling function that labels tweet as not spam if jargon occurs in it

    @labeling_function()
    def lf_contains_notspamwords(x):
        return NOT_SPAM if any(i in x.text for i in not_spam_words) else ABSTAIN

    # labelling fucntion that labels tweet as non spam if two or more jargon words in it

    @labeling_function()
    def count_ns_words(x):
        ns_count = 0
        for j in not_spam_words:
            counting = x.text.split().count(j)
            ns_count = ns_count + counting
        return NOT_SPAM if ns_count >= 2 else ABSTAIN

    # use labelling functions as input for Label Model

    lfs = [lf_contains_link, lf_contains_words, lf_contains_notspamwords, count_ns_words]

    applier = PandasLFApplier(lfs=lfs)
    L_train = applier.apply(df=df)

    label_model = LabelModel(cardinality=3, verbose=True)
    label_model.fit(L_train, n_epochs=100, seed=123, log_freq=20, l2=0.1, lr=0.01)


    # apply Snorkel labels to manually labelled data

    df["label_snorkel"] = label_model.predict(L=L_train, tie_break_policy="random")




