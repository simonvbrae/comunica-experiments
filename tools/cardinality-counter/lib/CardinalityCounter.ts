'use strict'

import { NamedNode, Quad } from 'rdf-js'
import { ParserOptions, WriterOptions, StreamParser, StreamWriter } from 'n3'
import { DataFactory } from 'rdf-data-factory'
import { readdir, lstat, Stats, ReadStream, createReadStream, WriteStream, createWriteStream } from 'node:fs'
import { extname, join } from 'node:path'
import { Readable } from 'node:stream'

const factory: DataFactory = new DataFactory()
const cardinalityPredicate: NamedNode = factory.namedNode('http://www.w3.org/2002/07/owl#cardinality')
const rdfIntegerType: NamedNode = factory.namedNode('http://www.w3.org/2001/XMLSchema#string')
const rdfFileExtensions: Set<string> = new Set(['.ttl', '.nt', '.nq'])

class CardinalityData {
  subject: Map<string, number> = new Map<string, number>()
  predicate: Map<string, number> = new Map<string, number>()

  //subject: Map<Quad_Subject, number> = new Map<Quad_Subject, number>()
  //predicate: Map<Quad_Predicate, number> = new Map<Quad_Predicate, number>()
  //object: Map<Quad_Object, number> = new Map<Quad_Object, number>()
  //graph: Map<Quad_Graph, number> = new Map<Quad_Graph, number>()

  public merge(other: CardinalityData): void {
    other.subject.forEach((value, key) => this.subject.set(key, (this.subject.get(key) ?? 0) + value))
    other.predicate.forEach((value, key) => this.predicate.set(key, (this.predicate.get(key) ?? 0) + value))
    //other.object.forEach((value, key) => this.object.set(key, (this.object.get(key) ?? 0) + value))
    //other.graph.forEach((value, key) => this.graph.set(key, (this.graph.get(key) ?? 0) + value))
  }

  public add(quad: Quad): void {
    if (quad.subject.termType == 'NamedNode') {
      this.subject.set(quad.subject.value, (this.subject.get(quad.subject.value) ?? 0) + 1)
    }

    if (quad.predicate.termType == 'NamedNode') {
      this.predicate.set(quad.predicate.value, (this.predicate.get(quad.predicate.value) ?? 0) + 1)
    }

    //this.object.set(quad.object, (this.object.get(quad.object) ?? 0) + 1)
    //this.graph.set(quad.graph, (this.graph.get(quad.graph) ?? 0) + 1)
  }

  public size(): number {
    return this.subject.size + this.predicate.size //+ this.object.size + this.graph.size
  }

  public *quads(): Generator<Quad> {
    let graph: NamedNode = factory.namedNode('#subject')
    for (const [quad, cardinality] of this.subject.entries()) {
      yield factory.quad(factory.namedNode(quad), cardinalityPredicate, factory.literal(cardinality.toString(), rdfIntegerType), graph)
    }
    graph = factory.namedNode('#predicate')
    for (const [quad, cardinality] of this.predicate.entries()) {
      yield factory.quad(factory.namedNode(quad), cardinalityPredicate, factory.literal(cardinality.toString(), rdfIntegerType), graph)
    }
    /*
    graph = factory.namedNode('#object')
    for (const [quad, cardinality] of this.object.entries()) {
      yield factory.quad(quad, cardinalityPredicate, factory.literal(cardinality.toString(), rdfIntegerType), graph)
    }
    graph = factory.namedNode('#graph')
    for (const [quad, cardinality] of this.graph.entries()) {
      yield factory.quad(quad, cardinalityPredicate, factory.literal(cardinality.toString(), rdfIntegerType), graph)
    }
    */
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
        //console.log(`Directory: ${path}`)
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
        //console.log(`File: ${path}`)
        const stream: ReadStream = createReadStream(path)
        const parser: StreamParser = new StreamParser(parserOptions)
          .on('data', (term: Quad) => {
            if (term.termType == 'Quad') {
              cardinalityData.add(term)
            }
          })
          .on('error', (err: Error) => reject(err))
          .on('end', () => resolve(cardinalityData))
        stream.pipe(parser)
      } else {
        reject(`Unknown file type: ${path}`)
      }
    })
  })
}

function serializeCardinalities(path: string, prefix: string, cardinalityData: CardinalityData, writerOptions: WriterOptions | undefined = undefined): Promise<string> {
  return new Promise<string>((resolve, reject) => {
    //console.log(`Serializing: ${cardinalityData.size()} > ${path}`)

    const quads: Readable = new Readable({ objectMode: true })
    const filePath: string = join(path, `${prefix}.nt`)
    const stream: WriteStream = createWriteStream(filePath)
    const writer: StreamWriter = new StreamWriter(writerOptions)

    quads
      .pipe(writer)
      .pipe(stream)
      .on('error', (err) => reject(err))
      .on('finish', () => resolve(filePath))

    for (const quad of cardinalityData.quads()) {
      quads.push(quad)
    }

    quads.push(null)
  })
}

export { getCardinalities, serializeCardinalities, CardinalityData }
