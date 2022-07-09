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

        this.node.core.on("connect", (id, name, headers) => {
            //send chain length to peer when they connect
            this.node.send({ to: id }, { length: this.chain.blocks.length }, 1000)
        })
        this.node.listen({ from: "" }, (data, id, name) => {
            // listen for responses from peers
            if (typeof data === 'object') {
                // if you receive a chain length longer than yours, request a block map
                if (data.length > this.chain.length) {
                    this.target_length = data.length
                    this.node.send({ to: id }, { request: "block_map" })
                }
                // if you get a request for a block map, map your blocks and respond with the map
                else if(data.request === "block_map") this.node.send({to: id}, {block_map: this.chain.mapBlocks()})
                else if(data.block_map) {
                    let missing_blocks = this.chain.compare(data.block_map)
                    missing_blocks.forEach(block_id => this.node.send("request_block", block_id))
                }
            }
        })
        this.node.listen("request_block", (block_id, name) => {
            if (name !== this.name) {
                let found = this.chain.blocks.slice().reverse().find(block => block.block_id === block_id)
                if (found) this.node.send("block", found)
            }
        })
        this.node.listen("requested_block", (block, name) => {
            console.log("requested_block!", block)
            if (name !== this.name) this.chain.add(block)
            // sort the all the new blocks after they all arrive
            if(this.chain.length >= this.target_length) this.chain.blocks = this.chain.sortBlocks(this.chain.blocks)
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