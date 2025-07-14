const { MongoClient } = require('mongodb');
const { faker } = require('@faker-js/faker');

// Configuración de conexión
const CONFIG = {
  uri: 'mongodb://localhost:27017',
  dbName: 'test_performance',
  collectionName: 'usuarios_test',
  totalRecords: 100000,
  batchSize: 1000 // Insertar en lotes para mejor rendimiento
};

// Función para generar datos falsos
function generateFakeUser(index) {
  return {
    id: index,
    nombre: faker.person.firstName(),
    apellido: faker.person.lastName(),
    email: faker.internet.email(),
    edad: faker.number.int({ min: 18, max: 80 }),
    telefono: faker.phone.number(),
    direccion: {
      calle: faker.location.streetAddress(),
      ciudad: faker.location.city(),
      pais: faker.location.country(),
      codigoPostal: faker.location.zipCode()
    },
    fechaRegistro: faker.date.recent({ days: 365 }),
    activo: faker.datatype.boolean(),
    salario: faker.number.float({ min: 30000, max: 120000, precision: 0.01 }),
    departamento: faker.helpers.arrayElement(['IT', 'Marketing', 'Ventas', 'RRHH', 'Finanzas']),
    habilidades: faker.helpers.arrayElements(
      ['JavaScript', 'Python', 'Java', 'C++', 'React', 'Node.js', 'MongoDB', 'SQL', 'Docker', 'AWS'],
      { min: 1, max: 5 }
    ),
    metadata: {
      ultimaConexion: faker.date.recent({ days: 30 }),
      sessionesActivas: faker.number.int({ min: 0, max: 5 }),
      puntuacion: faker.number.float({ min: 1, max: 5, precision: 0.1 })
    }
  };
}

async function insertBatch(collection, startIndex, batchSize) {
  const batch = [];
  for (let i = startIndex; i < startIndex + batchSize; i++) {
    batch.push(generateFakeUser(i));
  }
  
  try {
    const result = await collection.insertMany(batch, { ordered: false });
    return result.insertedCount;
  } catch (error) {
    console.error(`Error en lote ${startIndex}: ${error.message}`);
    return 0;
  }
}

function showProgress(inserted, total, startTime) {
  const elapsed = (Date.now() - startTime) / 1000;
  const rate = inserted / elapsed;
  const remaining = total - inserted;
  const eta = remaining / rate;
  
  process.stdout.clearLine();
  process.stdout.cursorTo(0);
  process.stdout.write(
    `Progreso: ${inserted}/${total} (${((inserted/total)*100).toFixed(1)}%) | ` +
    `Velocidad: ${rate.toFixed(0)} docs/s | ` +
    `ETA: ${eta.toFixed(0)}s`
  );
}

async function runPerformanceTest() {
  console.log('🚀 Iniciando test de rendimiento MongoDB');
  console.log(`📊 Insertando ${CONFIG.totalRecords.toLocaleString()} registros en lotes de ${CONFIG.batchSize}`);
  
  const client = new MongoClient(CONFIG.uri);
  
  try {
    await client.connect();
    console.log('✅ Conectado a MongoDB');
    
    const db = client.db(CONFIG.dbName);
    const collection = db.collection(CONFIG.collectionName);
    
    await collection.deleteMany({});
    console.log('🧹 Colección limpiada');
    
    await collection.createIndex({ email: 1 });
    await collection.createIndex({ departamento: 1 });
    await collection.createIndex({ edad: 1 });
    console.log('📋 Índices creados');
    
    const startTime = Date.now();
    let totalInserted = 0;
    
    for (let i = 0; i < CONFIG.totalRecords; i += CONFIG.batchSize) {
      const currentBatchSize = Math.min(CONFIG.batchSize, CONFIG.totalRecords - i);
      const inserted = await insertBatch(collection, i, currentBatchSize);
      totalInserted += inserted;
      
      showProgress(totalInserted, CONFIG.totalRecords, startTime);
    }
    
    const endTime = Date.now();
    const totalTime = (endTime - startTime) / 1000;
    
    console.log('\n\n📈 RESULTADOS DEL TEST:');
    console.log('========================');
    console.log(`✅ Registros insertados: ${totalInserted.toLocaleString()}`);
    console.log(`⏱️  Tiempo total: ${totalTime.toFixed(2)} segundos`);
    console.log(`🚀 Velocidad promedio: ${(totalInserted / totalTime).toFixed(0)} docs/segundo`);
    console.log(`💾 Base de datos: ${CONFIG.dbName}`);
    console.log(`📦 Colección: ${CONFIG.collectionName}`);
    
    const count = await collection.countDocuments();
    console.log(`✅ Verificación: ${count.toLocaleString()} documentos en la colección`);
    
    const stats = await db.stats();
    console.log(`📊 Tamaño de la BD: ${(stats.dataSize / 1024 / 1024).toFixed(2)} MB`);
    
    console.log('\n🔍 EJECUTANDO CONSULTAS DE TEST:');
    console.log('================================');
    
    let queryStart = Date.now();
    const userByEmail = await collection.findOne({ email: { $regex: /@/ } });
    let queryTime = Date.now() - queryStart;
    console.log(`📧 Búsqueda por email: ${queryTime}ms`);
    
    queryStart = Date.now();
    const deptCount = await collection.countDocuments({ departamento: 'IT' });
    queryTime = Date.now() - queryStart;
    console.log(`🏢 Conteo por departamento: ${queryTime}ms (${deptCount} registros)`);
    
    queryStart = Date.now();
    const ageRange = await collection.find({ edad: { $gte: 25, $lte: 35 } }).limit(10).toArray();
    queryTime = Date.now() - queryStart;
    console.log(`👥 Búsqueda por rango edad (25-35): ${queryTime}ms (${ageRange.length} resultados)`);
    
    queryStart = Date.now();
    const avgSalary = await collection.aggregate([
      { $group: { _id: '$departamento', salarioPromedio: { $avg: '$salario' } } }
    ]).toArray();
    queryTime = Date.now() - queryStart;
    console.log(`💰 Agregación salario promedio: ${queryTime}ms`);
    
  } catch (error) {
    console.error('❌ Error durante el test:', error.message);
  } finally {
    await client.close();
    console.log('\n🔌 Conexión cerrada');
  }
}

function showMenu() {
  console.log('\n⚙️  CONFIGURACIÓN ACTUAL:');
  console.log(`📊 Registros: ${CONFIG.totalRecords.toLocaleString()}`);
  console.log(`📦 Tamaño de lote: ${CONFIG.batchSize}`);
  console.log(`🔗 URI: ${CONFIG.uri}`);
  console.log(`🗃️  BD: ${CONFIG.dbName}`);
  console.log(`📋 Colección: ${CONFIG.collectionName}`);
  console.log('\n¿Deseas continuar con esta configuración? (s/n)');
}

if (require.main === module) {
  console.log('🧪 PROGRAMA DE TESTEO MONGODB');
  console.log('============================');
  
  try {
    require('mongodb');
    require('@faker-js/faker');
  } catch (error) {
    console.error('❌ Dependencias faltantes. Ejecuta:');
    console.error('npm install mongodb @faker-js/faker');
    process.exit(1);
  }
  
  showMenu();
  
  const readline = require('readline');
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });
  
  rl.question('', (answer) => {
    if (answer.toLowerCase() === 's' || answer.toLowerCase() === 'si' || answer === '') {
      rl.close();
      runPerformanceTest().catch(console.error);
    } else {
      console.log('❌ Test cancelado');
      rl.close();
    }
  });
}

module.exports = { runPerformanceTest, generateFakeUser, CONFIG };
