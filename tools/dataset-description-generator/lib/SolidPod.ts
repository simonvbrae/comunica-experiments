import { resolve, join, basename, extname } from 'node:path'
import { readdirSync, lstatSync, createReadStream, ReadStream, writeFile, unlinkSync, existsSync, readFile } from 'node:fs'
import { StreamParser, Parser, Writer } from 'n3'
import { NamedNode, Quad } from 'rdf-js'
import { DataFactory } from 'rdf-data-factory'

const factory: DataFactory = new DataFactory()

const defaultCustomIndexFile = 'voiddescription.nq'
const defaultCustomIndexFormat = 'application/n-quads'
const defaultProfileFormat = 'application/n-quads'

const predicates: Record<string, NamedNode> = {
  xsInteger: factory.namedNode('http://www.w3.org/2001/XMLSchema#integer'),
  voidTriples: factory.namedNode('http://rdfs.org/ns/void#triples'),
  voidDistinctSubjects: factory.namedNode('http://rdfs.org/ns/void#distinctSubjects'),
  voidDistinctObjects: factory.namedNode('http://rdfs.org/ns/void#distinctObjects'),
  voidProperties: factory.namedNode('http://rdfs.org/ns/void#properties'),
  voidProperty: factory.namedNode('http://rdfs.org/ns/void#property'),
  voidPropertyPartition: factory.namedNode('http://rdfs.org/ns/void#propertyPartition'),
  voidClassPartition: factory.namedNode('http://rdfs.org/ns/void#classPartition'),
  rdfType: factory.namedNode('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
  voidDataset: factory.namedNode('http://rdfs.org/ns/void#Dataset'),
  solidVoidDescription: factory.namedNode('http://www.w3.org/ns/solid/terms#voidDescription')
}

interface ISolidPod {
  path: string
  url: URL

  subjects: Map<string, number>
  predicates: Map<string, number>
  objects: Map<string, number>

  tripleCount: number

  process: () => Promise<void>
}

class SolidPod implements ISolidPod {
  public readonly path: string
  public readonly url: URL

  private readonly voidDescriptionPath: string
  private readonly profilePath: string
  private readonly profileIRI: URL

  public readonly subjects: Map<string, number>
  public readonly predicates: Map<string, number>
  public readonly objects: Map<string, number>

  public tripleCount: number

  constructor(path: string) {
    this.path = resolve(path)
    this.url = new URL(`http://localhost:3000/pods/${basename(path)}`)
    this.subjects = new Map<string, number>()
    this.predicates = new Map<string, number>()
    this.objects = new Map<string, number>()
    this.voidDescriptionPath = join(this.path, 'profile', defaultCustomIndexFile)
    this.profilePath = join(this.path, 'profile', 'card.nq')
    this.profileIRI = new URL(`${this.url}/profile/card#me`)
    this.tripleCount = 0
  }

  public async process(): Promise<void> {
    try {
      // console.log(`Process: ${this.path}`)
      await this.recursivelyProcessPod()
      await this.serializeVoidDescription()
    } catch (err) {
      console.log(`Failed: ${this.path}`, err)
    }
  }

  private async recursivelyProcessPod(path?: string): Promise<void> {
    const files: string[] = readdirSync(path ?? this.path)
    for (const file of files) {
      const filePath: string = join(path ?? this.path, file)
      const stats = lstatSync(filePath)
      if (stats.isDirectory()) {
        await this.recursivelyProcessPod(filePath)
      } else if (stats.isFile() && basename(filePath) !== defaultCustomIndexFile) { // do not count the custom index files
        await this.processFile(filePath)
      }/* else {
        console.log('Skip:', ${filePath})
      }*/
    }
  }

  private async processFile(path: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      // console.log(`Process file: ${path}`)
      const parser: StreamParser = new StreamParser({ factory: factory })
      const input: ReadStream = createReadStream(path)
      input.pipe(parser)
        .on('data', ({ subject, predicate, object }: Quad) => {
          if (subject.termType === 'NamedNode' || subject.termType === 'BlankNode') {
            const s: string = subject.value
            this.subjects.set(s, (this.subjects.get(s) ?? 0) + 1)
          }
          if (predicate.termType === 'NamedNode') {
            const p: string = predicate.value
            this.predicates.set(p, (this.predicates.get(p) ?? 0) + 1)
          }
          if (object.termType === 'NamedNode' || subject.termType === 'BlankNode') {
            const o: string = object.value
            this.objects.set(o, (this.objects.get(o) ?? 0) + 1)
          }
          this.tripleCount++
        })
        .on('error', reject)
        .on('end', resolve)
    })
  }

  private async serializeVoidDescription(): Promise<void> {
    return new Promise<void>((resolve, reject) => {

      const dataset: NamedNode = factory.namedNode(this.url.href)

      console.log(`Serialize index to ${this.voidDescriptionPath}`)

      if (existsSync(this.voidDescriptionPath)) {
        // console.log(`Delete old file at ${outputPath}`)
        unlinkSync(this.voidDescriptionPath)
      }

      const writer: Writer = new Writer({ format: defaultCustomIndexFormat })

      writer.addQuad(dataset, predicates.rdfType, predicates.voidDataset)
      writer.addQuad(dataset, predicates.voidTriples, factory.literal(this.tripleCount.toString(), predicates.xsInteger))
      writer.addQuad(dataset, predicates.voidDistinctSubjects, factory.literal(this.subjects.size.toString(), predicates.xsInteger))
      writer.addQuad(dataset, predicates.voidDistinctObjects, factory.literal(this.objects.size.toString(), predicates.xsInteger))
      writer.addQuad(dataset, predicates.voidProperties, factory.literal(this.predicates.size.toString(), predicates.xsInteger))

      for (const [iri, count] of this.predicates) {
        const propertyPartitionSubject = factory.blankNode()
        writer.addQuad(dataset, predicates.voidPropertyPartition, propertyPartitionSubject)
        writer.addQuad(propertyPartitionSubject, factory.namedNode(iri), factory.literal(count.toString(), predicates.xsInteger))
      }

      writer.end((err, result) => {
        if (err) {
          reject(err)
        } else {
          writeFile(this.voidDescriptionPath, result, (err) => err ? reject(err) : this.linkToProfile(this.voidDescriptionPath).then(resolve).catch(reject))
        }
      })
    })
  }

  private async linkToProfile(path: string, predicate?: NamedNode): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      
      const pathIri: URL = new URL(path.replace(this.path, this.url.href).replace(extname(path), '')) // strip '.nq' from the link

      const linkPredicate: NamedNode = predicate ?? predicates.voidDatasetDescription
      const profile: NamedNode = factory.namedNode(this.profileIRI.href)
      const target: NamedNode = factory.namedNode(pathIri.href)

      const parser: Parser = new Parser({ factory: factory })
      const writer: Writer = new Writer({ format: defaultProfileFormat })

      readFile(this.profilePath, 'utf8', (err, data) => {
        if (err) {
          reject(err)
        } else {
          const quads: Quad[] = parser.parse(data)
          const linkExists: boolean = quads.find(({ subject, predicate, object }: Quad) => subject === profile && predicate === linkPredicate && object === target) != null
          if (!linkExists) {
            quads.push(factory.quad(profile, linkPredicate, target))
          }
          writer.addQuads(quads)
          writer.end((err, result) => {
            if (err) {
              reject(err)
            } else {
              writeFile(this.profilePath, result, (err) => err ? reject(err) : resolve())
            }
          })
        }
      })
    })
  }
}

export { type ISolidPod, SolidPod }
