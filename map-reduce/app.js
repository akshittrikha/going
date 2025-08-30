const fs = require("fs");
const readline = require("readline");

if (process.argv.length < 3) {
  console.error("Usage: node app.js <input-file>");
  process.exit(1);
}

const inputPath = process.argv[2]

// ---- Types ----
// KeyValue: { key: string, value: number }

// ---- Mapper: takes a line, emits (word, 1) pairs ----
function mapper(line) {
  const out = [];
  // split on whitespace and basic punctuation,
  // lowercase for normalization
  const words = line
    .toLowerCase()
    .replace(/[^a-z0-9\s']/g, " ")
    .split(/\s+/)
    .filter(Boolean);

  for (const w of words) out.push({ key: w, value: 1 });
  return out;
}

// ---- Reducer: sums values for a key ----
function reducer(key, values) {
  const sum = values.reduce((a, b) => a + b, 0);
  return { key, value: sum};
}

// ---- Driver: Map -> Shuffle -> Reduce ----
async function run() {
  const startTime = performance.now(); // Start timing

  const rl = readline.createInterface({
    input: fs.createReadStream(inputPath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  // Map phase (streaming)
  const intermediate = []; // array of {key, value}
  for await (const line of rl) {
    const mapped = mapper(line);
    // push all pairs from this line
    for (const kv of mapped) intermediate.push(kv);
  }

  // Shuffle phase: group values by key
  const grouped = new Map(); // key -> number[]
  for (const {key, value } of intermediate)  {
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(value);
  }

  // Reduce phase
  const results = []
  for (const [key, values] of grouped.entries()) {
    results.push(reducer(key, values));
  }

  // Sort by descending cout, then alphabetically
  results.sort((a, b) => (b.value - a.value) || a.key.localeCompare(b.key));

  // Output
  for (const { key, value } of results) {
    console.log(`${key} -> ${value}`);
  }

  const endTime = performance.now(); // End timing
  const executionTime = endTime - startTime;
  console.log(`Execution time: ${executionTime.toFixed(2)} milliseconds`);
}

run().catch((err) => {
  console.error("Error:", err);
  process.exit(1);
});