const fs = require('fs');
const readline = require('readline');

class MinHeap {
    constructor() {
        this.heap = [];
    }

    parent(i) {
        return Math.floor((i - 1) / 2);
    }

    leftChild(i) {
        return 2 * i + 1;
    }

    rightChild(i) {
        return 2 * i + 2;
    }

    swap(i, j) {
        [this.heap[i], this.heap[j]] = [this.heap[j], this.heap[i]];
    }

    insert(score, id) {
        this.heap.push({ score, id });
        this.bubbleUp(this.heap.length - 1);
    }

    bubbleUp(i) {
        while (i > 0 && this.heap[this.parent(i)].score > this.heap[i].score) {
            this.swap(i, this.parent(i));
            i = this.parent(i);
        }
    }

    extractMin() {
        if (this.heap.length === 0) return null;

        const min = this.heap[0];
        const last = this.heap.pop();

        if (this.heap.length > 0) {
            this.heap[0] = last;
            this.bubbleDown(0);
        }

        return min;
    }

    bubbleDown(i) {
        let minIndex = i;
        const len = this.heap.length;

        while (true) {
            const left = this.leftChild(i);
            const right = this.rightChild(i);

            if (left < len && this.heap[left].score < this.heap[minIndex].score) {
                minIndex = left;
            }

            if (right < len && this.heap[right].score < this.heap[minIndex].score) {
                minIndex = right;
            }

            if (minIndex === i) break;

            this.swap(i, minIndex);
            i = minIndex;
        }
    }

    peek() {
        return this.heap[0];
    }

    size() {
        return this.heap.length;
    }
}

async function validateAndParseLine(line, lineNumber) {
    if (!line.trim()) return null;

    const colonIndex = line.indexOf(': ');
    if (colonIndex === -1) {
        throw new Error(`Invalid line format at line ${lineNumber}`);
    }

    const score = line.substring(0, colonIndex);
    const jsonPart = line.substring(colonIndex + 2);

    const numScore = parseInt(score);
    if (isNaN(numScore)) {
        throw new Error(`Invalid score format at line ${lineNumber}`);
    }

    let record;
    try {
        record = JSON.parse(jsonPart);
    } catch (e) {
        throw new Error(`Invalid JSON at line ${lineNumber}`);
    }

    if (!record.id || typeof record.id !== 'string') {
        throw new Error(`Missing or invalid 'id' field at line ${lineNumber}`);
    }

    return { score: numScore, id: record.id };
}

async function findTopNRecords(filePath, n) {
    const minHeap = new MinHeap();
    const fileStream = fs.createReadStream(filePath);

    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    let lineNumber = 0;
    try {
        for await (const line of rl) {
            lineNumber++;

            try {
                const record = await validateAndParseLine(line, lineNumber);
                if (!record) continue;

                if (minHeap.size() < n) {
                    minHeap.insert(record.score, record.id);
                } else if (record.score > minHeap.peek().score) {
                    minHeap.extractMin();
                    minHeap.insert(record.score, record.id);
                }
            } catch (error) {
                rl.close();
                fileStream.destroy();
                throw error;
            }
        }

        const results = minHeap.heap.sort((a, b) => b.score - a.score)
        return results;
    } catch (error) {
        throw error;
    }
}

async function main() {
    if (process.argv.length !== 4) {
        console.error('Usage: node <script.js> <filepath> <N>');
        process.exit(1);
    }

    const filePath = process.argv[2];
    const n = parseInt(process.argv[3]);

    if (isNaN(n) || n <= 0) {
        console.error('N must be a positive integer');
        process.exit(1);
    }

    try {
        try {
            await fs.promises.access(filePath, fs.constants.F_OK);
        } catch (error) {
            console.error('Error: Cannot find input file');
            process.exit(1);
        }

        try {
            await fs.promises.access(filePath, fs.constants.R_OK);
        } catch (error) {
            console.error('Error: Cannot read input file');
            process.exit(2);
        }

        const topRecords = await findTopNRecords(filePath, n);
        console.log(JSON.stringify(topRecords, null, 2));
    } catch (error) {
        console.error(`Error: ${error.message}`);
        process.exit(2);
    }
}

main();