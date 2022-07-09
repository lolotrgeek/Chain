const { randomUUID } = require('crypto')
const { Chain } = require('./chain')
const { Node } = require('./node')

/**
 * An immutable distributed key value store with eventual consistency.
 */
class Ledger {
    constructor(name) {
        this.name = name ? name : randomUUID()
        this.chain = new Chain()
        this.node = new Node(this.name)

        this.chain_lengths = []
        this.missing_blocks = []
        this.received_blocks = []

        this.requestMissingBlocks = block_map => {
            this.missing_blocks = this.chain.compare(block_map)
            this.missing_blocks.forEach(block_id => this.node.send("request_block", block_id))
        }
        this.respondBlockMap = id => this.node.send({ to: id }, { block_map: this.chain.mapBlocks() })
        this.requestBlockMap = id => this.node.send({ to: id }, { request: "block_map" })
        this.addChainLength = (id, length) => this.chain_lengths.push({ id, length })
        this.selectChain = chain => { if (chain.length > this.chain.length) this.requestBlockMap(chain.id) }

        //send chain length to peer when they connect
        this.node.core.on("connect", (id, name, headers) => this.node.send({ to: id }, { length: this.chain.blocks.length }))
        this.node.listen({ from: "" }, (data, id, name) => {
            if (typeof data === 'object') {
                if (data.length) this.addChainLength(id, data.length)
                else if (data.request === "block_map") this.respondBlockMap(id)
                else if (data.block_map) this.requestMissingBlocks(data.block_map)
            }
            if (this.chain_lengths.length >= Object.keys(this.core.getPeers()).length) {
                // only request block map from longest chain
                this.chain_lengths.forEach(chain => this.selectChain(chain))
            }
        })
        this.node.listen("request_block", (block_id, name) => {
            if (name !== this.name) {
                let found = this.chain.blocks.slice().reverse().find(block => block.block_id === block_id)
                if (found) this.node.send("block", found)

            }
        })
        this.node.listen("requested_block", (block, name) => {
            console.log("received block!", block)
            if (name !== this.name) {
                this.received_blocks.push(block)
                this.chain.add(block)
            }
            if (this.missing_blocks.length === this.received_blocks.length) {
                // sort the new blocks after they all arrive
                this.chain.blocks = this.chain.sortBlocks(this.chain.blocks)
                this.missing_blocks = []
                this.received_blocks = []
            }
        })

        this.node.listen("block", (block, name) => {
            console.log("block!", block)
            if (name !== this.name) this.chain.add(block)
        })

        this.node.listen("chain", (chain, name) => {
            if (name !== this.name) {
                console.log(name, '->', this.name)
                this.chain.merge(chain)
            }
        })

        this.node.listen("request", (key, name) => {
            if (name !== this.name) {
                let found = this.chain.blocks.slice().reverse().find(block => block.data.key === key)
                if (found) this.node.send("block", found)
            }
        })

        this.errors = []
        this.retries = 3
        this.tries = 0
    }

    /**
     * Insert the given key value pair as a new block on the chain.
     * @param {*} key 
     * @param {*} value 
     * @returns 
     */
    put(key, value) {
        let block = this.chain.put({ key, value })
        this.node.send("block", block)
        return block
    }

    /**
     * Find the latest value of the given key.
     * @param {*} key 
     * @returns 
     * @note we assume that the latest blocks hold the most updated data.
     */
    get(key) {
        try {
            let found = this.chain.blocks.slice().reverse().find(block => block.data.key === key)
            if (found) {
                this.tries = 0
                console.log("found", found.data)
                return found.data
            }
            else if (!found && this.tries < this.retries) {
                console.log('looking...')
                this.tries++
                this.node.send("request", key) // NOTE: using this we do not need to have the entire chain, we can get blocks as needed.
                setTimeout(() => this.get(key), this.tries * 500)
            }
            else {
                return "unable to find."
            }

        } catch (error) {
            this.errors.push(error)
        }

    }

    /**
     * Find all entries of the given key on the chain.
     * @param {*} key 
     * @returns 
     */
    get_history(key) {
        try {
            return this.chain.blocks.filter(block => block.data.key === key).map(block => block.data)
        } catch (error) {
            this.errors.push(error)
        }

    }

    /**
     * Retrieves all the latest entries.
     * @returns 
     */
    get_all() {
        try {
            const entries = this.chain.blocks.map(block => block.data).reverse()
            const latest = new Set()
            const latest_entries = entries.filter(entry => {
                const isDuplicate = latest.has(entry.key)
                latest.add(entry.key)
                if (!isDuplicate) return true
                return false
            })
            return latest_entries
        } catch (error) {
            this.errors.push(error)
        }

    }

}

module.exports = { Ledger }