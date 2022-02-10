import { ConnectionOptions, Mongoose } from 'mongoose';
import { MongooseSchema } from './MongooseSchema';
import { schemaConverter } from './SchemaConverter';
import { ConduitSchema, GrpcError } from '@conduitplatform/conduit-grpc-sdk';
import { systemRequiredValidator } from '../utils/validateSchemas';
import { DatabaseAdapter } from '../DatabaseAdapter';
import { status} from '@grpc/grpc-js';

let deepPopulate = require('mongoose-deep-populate');

export class MongooseAdapter extends DatabaseAdapter<MongooseSchema> {
  connected: boolean = false;
  mongoose: Mongoose;
  connectionString: string;
  options: ConnectionOptions = {
    keepAlive: true,
    poolSize: 10,
    connectTimeoutMS: 30000,
    useNewUrlParser: true,
    useCreateIndex: true,
    useFindAndModify: false,
    useUnifiedTopology: true,
  };

  registeredSchemas: Map<string, ConduitSchema>;

  constructor(connectionString: string) {
    super();
    this.registeredSchemas = new Map();
    this.connectionString = connectionString;
    this.mongoose = new Mongoose();
    this.connect();
  }

  async ensureConnected(): Promise<any> {
    return new Promise((resolve, reject) => {
      let db = this.mongoose.connection;
      db.on('connected', () => {
        console.log('MongoDB: Database is connected');
        resolve();
      });

      db.on('error', (err: any) => {
        console.error('MongoDB: Connection error:', err.message);
        reject();
      });

      db.once('open', function callback() {
        console.info('MongoDB: Connection open!');
        resolve();
      });

      db.on('reconnected', function () {
        console.log('MongoDB: Database reconnected!');
        resolve();
      });

      db.on('disconnected', function () {
        console.log('MongoDB: Database Disconnected');
        reject();
      });
    });
  }

  connect() {
    this.mongoose
      .connect(this.connectionString, this.options)
      .then(() => {
        deepPopulate = deepPopulate(this.mongoose);
      })
      .catch((err: any) => {
        console.log(err);
        throw new GrpcError(status.INTERNAL, 'Connection with Mongo not possible');
      });
  }

  extensionFieldsExist(schema: ConduitSchema, fields: ConduitSchema['fields'], extOwner: string) {
    for (const field of Object.keys(fields)) {
      if (field in schema.fields) return true;
    }
    if (schema.schemaOptions.conduit && schema.schemaOptions.conduit.extensions) {
      for (const ext of schema.schemaOptions.conduit.extensions) {
        if (ext.ownerModule === extOwner) continue;
        for (const field of Object.keys(fields)) {
          if (field in ext.fields) return true;
        }
      }
    }
    return false;
  }

  async createSchemaFromAdapter(schema: ConduitSchema): Promise<MongooseSchema> {
    if (!this.models) {
      this.models = {};
    }

    if (this.registeredSchemas.has(schema.name)) {
      if (schema.name !== 'Config') {
        schema = systemRequiredValidator(
          this.registeredSchemas.get(schema.name)!,
          schema
        );
        // TODO this is a temporary solution because there was an error on updated config schema for invalid schema fields
      }
      delete this.mongoose.connection.models[schema.name];
    }
    const owned = await this.checkModelOwnership(schema);
    let declaredSchema: ConduitSchema;
    let newSchema: ConduitSchema;
    if (owned) {
      this.addSchemaPermissions(schema);
      declaredSchema = schema;
      newSchema = schemaConverter(schema);
    } else {
      // Schema Extensions
      delete schema.fields._id;
      delete schema.fields.createdAt;
      delete schema.fields.updatedAt;
      declaredSchema = this.registeredSchemas.get(schema.name)!;
      if (declaredSchema.name === '_DeclaredSchema' ||
          !declaredSchema.schemaOptions.conduit ||
          !declaredSchema.schemaOptions.conduit!.permissions ||
          !declaredSchema.schemaOptions.conduit!.permissions!.extendable) {
        throw new GrpcError(status.PERMISSION_DENIED, 'Schema already exists and is not extendable');
      }
      if (!declaredSchema.schemaOptions.conduit.extensions) {
        declaredSchema.schemaOptions.conduit.extensions = [];
      }
      const index = declaredSchema.schemaOptions.conduit.extensions.findIndex((ext: any) => ext.ownerModule === schema.ownerModule);
      if (index === -1) {
        // Create Extension
        if (Object.keys(schema.fields).length === 0) {
          throw new GrpcError(status.INVALID_ARGUMENT, 'Could not create schema extension with no custom fields');
        }
        if (this.extensionFieldsExist(declaredSchema, schema.fields, schema.ownerModule)) {
          throw new GrpcError(status.ALREADY_EXISTS, 'Could not create schema extension due to duplicate fields');
        }
        declaredSchema.schemaOptions.conduit.extensions.push({
          fields: schema.fields,
          ownerModule: schema.ownerModule,
          createdAt: new Date(), // TODO FORMAT
          updatedAt: new Date(), // TODO FORMAT
        });
      } else {
        if (Object.keys(schema.fields).length === 0) {
          declaredSchema.schemaOptions.conduit.extensions.splice(index, 1);
        } else {
          // Update Extension
          if (this.extensionFieldsExist(declaredSchema, schema.fields, schema.ownerModule)) {
            throw new GrpcError(status.ALREADY_EXISTS, 'Could not update schema extension due to duplicate fields');
          }
          declaredSchema.schemaOptions.conduit.extensions[index].fields = schema.fields;
          declaredSchema.schemaOptions.conduit.extensions[index].fields.updatedAt = new Date(); // TODO FORMAT
        }
      }
    }

    newSchema = schemaConverter(declaredSchema);
    this.registeredSchemas.set(declaredSchema.name, declaredSchema);
    this.models[declaredSchema.name] = new MongooseSchema(
      this.mongoose,
      newSchema,
      declaredSchema,
      deepPopulate,
      this
    );
    if (declaredSchema.name !== '_DeclaredSchema') {
      await this.saveSchemaToDatabase(declaredSchema);
    }

    return this.models![declaredSchema.name];
  }

  getSchema(schemaName: string): ConduitSchema {
    if (this.models && this.models![schemaName]) {
      return this.models![schemaName].originalSchema;
    }
    throw new GrpcError(status.NOT_FOUND, `Schema ${schemaName} not defined yet`);
  }

  getSchemaStitched(schemaName: string): ConduitSchema {
    if (this.models && this.models![schemaName]) {
      const schema = this.models![schemaName].originalSchema;
      if (schema.schemaOptions.conduit && schema.schemaOptions.conduit!.extensions) {
        schema.schemaOptions.conduit!.extensions.forEach((ext: any) => {
          for (const field of Object.keys(ext.fields)) {
            schema.fields[field] = ext.fields[field];
          }
        });
      }
      return schema;
    }
    throw new GrpcError(status.NOT_FOUND, `Schema ${schemaName} not defined yet`);
  }

  getSchemas(): ConduitSchema[] {
    if (!this.models) {
      return [];
    }
    const self = this;
    return Object.keys(this.models).map((modelName) => {
      return self.models![modelName].originalSchema;
    });
  }

  getSchemasStitched(): ConduitSchema[] {
    if (!this.models) {
      return [];
    }
    const self = this;
    return Object.keys(this.models).map((modelName) => {
      const schema = self.models![modelName].originalSchema;
      if (schema.schemaOptions.conduit && schema.schemaOptions.conduit!.extensions) {
        schema.schemaOptions.conduit!.extensions.forEach((ext: any) => {
          for (const field of Object.keys(ext.fields)) {
            schema.fields[field] = ext.fields[field];
          }
        });
      }
      return schema;
    });
  }

  getSchemaModel(schemaName: string): { model: MongooseSchema; relations: any } {
    if (this.models && this.models![schemaName]) {
      return { model: this.models![schemaName], relations: null };
    }
    throw new GrpcError(status.NOT_FOUND, `Schema ${schemaName} not defined yet`);
  }

  async deleteSchema(schemaName: string, deleteData: boolean, callerModule: string = 'database'): Promise<string> {
    if (!this.models?.[schemaName])
      throw new GrpcError(status.NOT_FOUND, 'Requested schema not found');
    if (
      (this.models[schemaName].originalSchema.ownerModule !== callerModule) &&
      (this.models[schemaName].originalSchema.name !== 'SchemaDefinitions') // SchemaDefinitions migration
    ) {
      throw new GrpcError(status.PERMISSION_DENIED, 'Not authorized to delete schema');
    }
    if (deleteData) {
      await this.models![schemaName].model.collection
        .drop()
        .catch((e: Error) => { throw new GrpcError(status.INTERNAL, e.message); });
    }
    this.models!['_DeclaredSchema']
      .findOne(JSON.stringify({ name: schemaName }))
      .then( model => {
        if (model) {
          this.models!['_DeclaredSchema']
            .deleteOne(JSON.stringify({name: schemaName}))
            .catch((e: Error) => { throw new GrpcError(status.INTERNAL, e.message); })
        }
      });

    delete this.models![schemaName];
    delete this.mongoose.connection.models[schemaName];
    return 'Schema deleted!';
    // TODO should we delete anything else?
  }
}
