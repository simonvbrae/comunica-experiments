'use strict'

import { NamedNode, Quad, Quad_Subject, Quad_Predicate, Quad_Object, Quad_Graph } from 'rdf-js'
import { ParserOptions, WriterOptions, StreamParser, StreamWriter } from 'n3'
import { DataFactory } from 'rdf-data-factory'
import { readdir, lstat, Stats, ReadStream, createReadStream, WriteStream, createWriteStream } from 'node:fs'
import { extname, join } from 'node:path'
import { Readable } from 'node:stream'

const factory: DataFactory = new DataFactory()
const cardinalityPredicate: NamedNode = factory.namedNode('http://www.w3.org/2002/07/owl#cardinality')
const rdfIntegerType: NamedNode = factory.namedNode('http://www.w3.org/2001/XMLSchema#integer')
const rdfFileExtensions: Set<string> = new Set(['.ttl', '.nt', '.nq'])

class CardinalityData {
  subject: Map<Quad_Subject, number> = new Map<Quad_Subject, number>()
  predicate: Map<Quad_Predicate, number> = new Map<Quad_Predicate, number>()
  object: Map<Quad_Object, number> = new Map<Quad_Object, number>()
  graph: Map<Quad_Graph, number> = new Map<Quad_Graph, number>()

  public merge(other: CardinalityData): void {
    other.subject.forEach((value, key) => this.subject.set(key, (this.subject.get(key) ?? 0) + value))
    other.predicate.forEach((value, key) => this.predicate.set(key, (this.predicate.get(key) ?? 0) + value))
    other.object.forEach((value, key) => this.object.set(key, (this.object.get(key) ?? 0) + value))
    other.graph.forEach((value, key) => this.graph.set(key, (this.graph.get(key) ?? 0) + value))
  }

  public add(quad: Quad): void {
    this.subject.set(quad.subject, (this.subject.get(quad.subject) ?? 0) + 1)
    this.predicate.set(quad.predicate, (this.predicate.get(quad.predicate) ?? 0) + 1)
    this.object.set(quad.object, (this.object.get(quad.object) ?? 0) + 1)
    this.graph.set(quad.graph, (this.graph.get(quad.graph) ?? 0) + 1)
  }

  public size(): number {
    return this.subject.size + this.predicate.size + this.object.size + this.graph.size
  }
}

function hasValidFileExtension(path: string): boolean {
  const ext = extname(path)
  return ext ? rdfFileExtensions.has(ext) : false
}

async function getCardinalities(path: string, parserOptions: ParserOptions | undefined = undefined): Promise<CardinalityData> {
  return new Promise<CardinalityData>((resolve, reject) => {
    const cardinalityData: CardinalityData = new CardinalityData()
    lstat(path, (err: NodeJS.ErrnoException | null, stats: Stats) => {
      if (err) {
        reject(err)
      } else if (stats.isDirectory()) {
        console.log(`Directory: ${path}`)
        readdir(path, (err, files) => {
          if (err) {
            reject(err)
          }
          const promises: Promise<CardinalityData>[] = files.map((file) => getCardinalities(join(path, file), parserOptions))
          Promise.all(promises).then((data) => {
            data.forEach((d: CardinalityData) => cardinalityData.merge(d))
            resolve(cardinalityData)
          })
        })
      } else if (hasValidFileExtension(path)) {
        console.log(`File: ${path}`)
        const stream: ReadStream = createReadStream(path)
        const parser: StreamParser = new StreamParser(parserOptions)
          .on('data', (quad: Quad) => cardinalityData.add(quad))
          .on('error', (err: Error) => reject(err))
          .on('end', () => resolve(cardinalityData))
        stream.pipe(parser)
      } else {
        reject(`Unknown file type: ${path}`)
      }
    })
  })
}

function serializeCardinalities(
  path: string,
  prefix: string,
  cardinalityData: CardinalityData,
  writerOptions: WriterOptions | undefined = undefined
): Promise<string> {
  return new Promise<string>((resolve, reject) => {
    console.log(`Serializing: ${cardinalityData.size()} > ${path}, prefix ${prefix}`)

    const subjectQuads: Readable = new Readable({ objectMode: true })
    const subjectPath: string = join(path, `${prefix}.subject.nt`)
    const subjectStream: WriteStream = createWriteStream(subjectPath)
    const subjectWriter: StreamWriter = new StreamWriter(writerOptions)

    cardinalityData.subject.forEach((value, key) =>
      subjectQuads.push(factory.quad(key, cardinalityPredicate, factory.literal(value.toString(), rdfIntegerType)))
    )
    subjectQuads.push(null)

    subjectQuads
      .pipe(subjectWriter)
      .pipe(subjectStream)
      .on('error', (err) => reject(err))
      .on('finish', () => resolve(subjectPath))
  })
}

export { getCardinalities, serializeCardinalities }
