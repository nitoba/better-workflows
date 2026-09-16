/**
 * Check the documentation users see through the published declaration graph.
 * Uses the locked TypeScript 7 compiler API (development tooling only). Its sync
 * transport requires Node, so invoke with `bun run docs:check`, not `bun <file>`.
 */
import { API, SymbolFlags, SignatureKind } from 'typescript/unstable/sync'
import { SyntaxKind } from 'typescript/unstable/ast'
import { readFile, readdir, mkdtemp, writeFile, rm } from 'node:fs/promises'
import { dirname, resolve, relative, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { spawnSync } from 'node:child_process'
import assert from 'node:assert/strict'

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const manifest = JSON.parse(await readFile(join(root, 'package.json'), 'utf8'))
const entries = Object.entries(manifest.exports)
  .filter(([, value]) => value.types)
  .map(([key, value]) => ({
    name: key === '.' ? manifest.name : `${manifest.name}/${key.slice(2)}`,
    source: join(root, 'src', key === '.' ? 'index.ts' : `${key.slice(2)}.ts`),
    emitted: resolve(root, value.types)
  }))
const directory = await mkdtemp(join(root, '.jsdoc-check-'))
const failures = []
const sources = (await readdir(join(root, 'src'))).filter((name) => name.endsWith('.ts'))
let examples = 0
let documented = 0
let api

function within(path, base) {
  return path === base || path.startsWith(`${base}/`) || path.startsWith(`${base}\\`)
}

function collect(project, entry, base) {
  const checker = project.checker
  const source = project.program.getSourceFile(entry)
  assert(source, `Entry point is missing from the consumer graph: ${entry}`)
  const moduleSymbol = checker.getSymbolAtLocation(source)
  assert(moduleSymbol, `No module symbol for ${entry}`)
  const docs = new Map()

  function inspect(symbol, label, ancestors = new Set()) {
    if (symbol.flags & SymbolFlags.Alias) symbol = checker.getAliasedSymbol(symbol)
    if (ancestors.has(symbol.id) || docs.has(label)) return
    const declaration = symbol.declarations.find((node) => within(node.path, base))
    if (!declaration) return // Nest/JS inherited members are documented by their packages.
    const node = declaration.resolve()
    assert(node, `Declaration is unavailable: ${label}`)
    if (
      node.modifiers?.some(
        (modifier) =>
          modifier.kind === SyntaxKind.PrivateKeyword ||
          modifier.kind === SyntaxKind.ProtectedKeyword
      )
    )
      return
    const tags = symbol.getJsDocTags(checker)
    if (tags.some((tag) => tag.name === 'internal')) return
    const children = new Set(ancestors).add(symbol.id)
    if (symbol.name === '__constructor' || symbol.name === 'prototype') return
    const description = symbol.getDocumentationComment(checker).trim()
    if (!description) failures.push(`${label}: missing JSDoc summary in ${relative(root, entry)}`)
    docs.set(label, {
      description,
      // Retain parameter/default/example text as well as hover summaries.
      tags: tags.map((tag) => ({ name: tag.name, text: tag.text }))
    })

    const rawType = checker.getTypeOfSymbol(symbol)
    const valueType = rawType && checker.getNonNullableType(rawType)
    if (valueType) {
      for (const signature of checker.getSignaturesOfType(valueType, SignatureKind.Call)) {
        for (const parameter of signature.getParameters()) {
          const name = parameter.name
          const documented = tags.some(
            (tag) => tag.name === 'param' && tag.text?.split(/\s/, 1)[0] === name
          )
          if (!documented) failures.push(`${label}: missing @param ${name}`)
        }
        if (!tags.some((tag) => tag.name === 'returns')) failures.push(`${label}: missing @returns`)
      }
      // Object-valued groups (admin.migrations) and static class methods.
      for (const property of checker.getPropertiesOfType(
        checker.getNonNullableType(valueType) ?? valueType
      )) {
        inspect(property, `${label}.${property.name}`, children)
      }
    }
    // Instance members and fields of exported interfaces, including inherited ones.
    if (symbol.flags & (SymbolFlags.Class | SymbolFlags.Interface | SymbolFlags.TypeAlias)) {
      const declaredType = checker.getDeclaredTypeOfSymbol(symbol)
      for (const property of checker.getPropertiesOfType(declaredType))
        inspect(property, `${label}.${property.name}`, children)
    }
  }
  for (const symbol of checker.getExportsOfModule(moduleSymbol)) inspect(symbol, symbol.name)
  return docs
}

try {
  // Extract real @example fences, never duplicate examples into a separate test file.
  for (const name of sources) {
    const text = await readFile(join(root, 'src', name), 'utf8')
    for (const block of text.matchAll(/\/\*\*([\s\S]*?)\*\//g)) {
      const content = block[1].replace(/^\s*\* ?/gm, '')
      if (!content.includes('@example')) continue
      const fences = [...content.matchAll(/```(?:ts|typescript)\s*\n([\s\S]*?)```/g)]
      assert(fences.length, `${name}: @example must contain a TypeScript fence`)
      for (const [, code] of fences) {
        // Prevent examples from silently opting out of the compiler check.
        assert(!/@ts-(?:ignore|nocheck|expect-error)/.test(code), `${name}: unchecked @example`)
        await writeFile(join(directory, `${name.replace('.ts', '')}-${++examples}.mts`), code)
      }
    }
  }
  assert(examples > 0, 'No JSDoc examples found')
  await writeFile(
    join(directory, 'entrypoints.mts'),
    entries.map((entry, i) => `export * as entry${i} from ${JSON.stringify(entry.name)}`).join('\n')
  )
  const config = join(directory, 'tsconfig.json')
  await writeFile(
    config,
    JSON.stringify(
      {
        compilerOptions: {
          target: 'ES2023',
          module: 'NodeNext',
          moduleResolution: 'NodeNext',
          types: ['node', 'bun'],
          strict: true,
          skipLibCheck: true,
          experimentalDecorators: true,
          emitDecoratorMetadata: true,
          noEmit: true,
          noUncheckedIndexedAccess: true,
          exactOptionalPropertyTypes: true
        },
        include: ['./*.mts']
      },
      null,
      2
    )
  )
  const compiler = spawnSync(
    process.execPath,
    [join(root, 'node_modules/typescript/bin/tsc'), '-p', config],
    { cwd: root, encoding: 'utf8' }
  )
  if (compiler.error) throw compiler.error
  if (compiler.status !== 0)
    throw new Error(
      `JSDoc examples do not type-check against package exports:\n${compiler.stdout}${compiler.stderr}`
    )

  api = new API({ cwd: root })
  const snapshot = api.updateSnapshot({ openProjects: [join(root, 'tsconfig.json'), config] })
  try {
    const sourceProject = snapshot.getProject(join(root, 'tsconfig.json'))
    const consumerProject = snapshot.getProject(config)
    assert(sourceProject && consumerProject, 'Compiler projects were not loaded')
    for (const entry of entries) {
      const sourceDocs = collect(sourceProject, entry.source, join(root, 'src'))
      const emittedDocs = collect(consumerProject, entry.emitted, join(root, 'dist'))
      for (const [label, doc] of sourceDocs) {
        const emitted = emittedDocs.get(label)
        if (!emitted)
          failures.push(`${entry.name}.${label}: documentation missing from emitted API`)
        else if (JSON.stringify(emitted) !== JSON.stringify(doc)) {
          failures.push(
            `${entry.name}.${label}: JSDoc changed or disappeared during declaration bundling`
          )
        }
      }
      documented += sourceDocs.size
      console.log(
        `${entry.name}: ${sourceDocs.size} documented exports/members checked in source and declarations`
      )
    }
  } finally {
    snapshot.dispose()
  }
  if (failures.length)
    throw new Error(`JSDoc verification failed:\n${[...new Set(failures)].join('\n')}`)
  console.log(
    `JSDoc verified: ${documented} export/member checks, ${examples} type-checked examples, ${entries.length} public entry points.`
  )
} finally {
  api?.close()
  await rm(directory, { recursive: true, force: true })
}
