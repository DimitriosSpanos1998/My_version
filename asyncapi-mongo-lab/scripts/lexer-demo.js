const { spawnSync } = require("child_process");
const fs = require("fs");
const path = require("path");

const repoRoot = path.resolve(__dirname, "..");
const lexerDir = path.join(repoRoot, "lexer");
const srcDir = path.join(lexerDir, "oaql2");
const flexFile = path.join(lexerDir, "Lexer.flex");
const generatedLexer = path.join(srcDir, "Lexer.java");
const buildDir = path.join(lexerDir, "build");

const cupJar = process.env.CUP_RUNTIME_JAR || process.env.CUP_JAR;

function ensureBinary(binaryName, installHint) {
  const result = spawnSync("which", [binaryName], { stdio: "ignore" });
  if (result.status !== 0) {
    throw new Error(
      `Required tool not found on PATH: ${binaryName}. ${installHint || "Please install it and try again."}`
    );
  }
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, { stdio: "inherit", ...options });
  if (result.status !== 0) {
    throw new Error(`Command failed: ${command} ${args.join(" ")}`);
  }
}

function shouldRegenerateLexer() {
  if (!fs.existsSync(generatedLexer)) {
    return true;
  }
  const flexStat = fs.statSync(flexFile);
  const lexerStat = fs.statSync(generatedLexer);
  return flexStat.mtimeMs > lexerStat.mtimeMs;
}

function main() {
  ensureBinary("java", "Install a JDK (Java 11+) and ensure java is available.");
  ensureBinary("javac", "Install a JDK (Java 11+) and ensure javac is available.");
  ensureBinary("jflex", "Install JFlex and ensure jflex is available.");

  if (!cupJar || !fs.existsSync(cupJar)) {
    throw new Error(
      "CUP runtime jar not found. Set CUP_RUNTIME_JAR (or CUP_JAR) to the path of java-cup runtime jar."
    );
  }

  if (shouldRegenerateLexer()) {
    run("jflex", ["-d", srcDir, flexFile]);
  }

  if (!fs.existsSync(buildDir)) {
    fs.mkdirSync(buildDir, { recursive: true });
  }

  const javaSources = [
    path.join(srcDir, "Lexer.java"),
    path.join(srcDir, "sym.java"),
    path.join(srcDir, "SimpleParser.java"),
    path.join(srcDir, "ParserDemo.java"),
  ];

  run("javac", ["-cp", cupJar, "-d", buildDir, ...javaSources]);
  run("java", ["-cp", `${buildDir}${path.delimiter}${cupJar}`, "oaql2.ParserDemo"]);
}

try {
  main();
} catch (error) {
  console.error(error.message || error);
  process.exit(1);
}
