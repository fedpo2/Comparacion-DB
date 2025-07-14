const mysql = require('mysql2/promise');
const { faker } = require('@faker-js/faker');

// Configuración de conexión
const CONFIG = {
  host: 'localhost',
  port: 3306,
  user: 'test',
  password: 'testtest', // Cambiar según tu configuración
  database: 'test_performance',
  totalRecords: 100000,
  batchSize: 1000, // Insertar en lotes para mejor rendimiento
  tableName: 'usuarios_test'
};

// SQL para crear la tabla
const CREATE_TABLE_SQL = `
CREATE TABLE IF NOT EXISTS ${CONFIG.tableName} (
  id INT AUTO_INCREMENT PRIMARY KEY,
  usuario_id INT NOT NULL,
  nombre VARCHAR(100) NOT NULL,
  apellido VARCHAR(100) NOT NULL,
  email VARCHAR(255) NOT NULL,
  edad INT NOT NULL,
  direccion_calle VARCHAR(255),
  direccion_ciudad VARCHAR(100),
  direccion_pais VARCHAR(100),
  direccion_codigo_postal VARCHAR(20),
  fecha_registro DATETIME NOT NULL,
  activo BOOLEAN DEFAULT TRUE,
  salario DECIMAL(10,2),
  departamento VARCHAR(50),
  habilidades JSON,
  ultima_conexion DATETIME,
  sesiones_activas INT DEFAULT 0,
  puntuacion DECIMAL(3,2),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  
  INDEX idx_email (email),
  INDEX idx_departamento (departamento),
  INDEX idx_edad (edad),
  INDEX idx_fecha_registro (fecha_registro),
  INDEX idx_activo (activo)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
`;

function generateFakeUser(index) {
  const habilidades = faker.helpers.arrayElements(
    ['JavaScript', 'Python', 'Java', 'C++', 'React', 'Node.js', 'MySQL', 'SQL', 'Docker', 'AWS'],
    { min: 1, max: 5 }
  );
  
  return [
    index, // usuario_id
    faker.person.firstName(),
    faker.person.lastName(),
    faker.internet.email(),
    faker.number.int({ min: 18, max: 80 }),
    faker.location.streetAddress(),
    faker.location.city(),
    faker.location.country(),
    faker.location.zipCode(),
    faker.date.recent({ days: 365 }),
    faker.datatype.boolean(),
    faker.number.float({ min: 30000, max: 120000, precision: 0.01 }),
    faker.helpers.arrayElement(['IT', 'Marketing', 'Ventas', 'RRHH', 'Finanzas']),
    JSON.stringify(habilidades),
    faker.date.recent({ days: 30 }),
    faker.number.int({ min: 0, max: 5 }),
    faker.number.float({ min: 1, max: 5, precision: 0.1 })
  ];
}

async function insertBatch(connection, startIndex, batchSize) {
  const values = [];
  const placeholders = [];
  
  for (let i = startIndex; i < startIndex + batchSize; i++) {
    const userData = generateFakeUser(i);
    values.push(...userData);
    placeholders.push('(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
  }
  
  const sql = `
    INSERT INTO ${CONFIG.tableName} 
    (usuario_id, nombre, apellido, email, edad, direccion_calle, 
     direccion_ciudad, direccion_pais, direccion_codigo_postal, fecha_registro, 
     activo, salario, departamento, habilidades, ultima_conexion, sesiones_activas, puntuacion)
    VALUES ${placeholders.join(', ')}
  `;
  
  try {
    const [result] = await connection.execute(sql, values);
    return result.affectedRows;
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

async function getTableStats(connection) {
  const [rows] = await connection.query(`
    SELECT 
      TABLE_NAME,
      ROUND(((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024), 2) AS 'Tamaño_MB',
      TABLE_ROWS as 'Filas_Estimadas',
      AUTO_INCREMENT as 'Próximo_ID',
      ENGINE,
      TABLE_COLLATION
    FROM information_schema.TABLES 
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
  `, [CONFIG.database, CONFIG.tableName]);
  
  return rows[0];
}

async function runPerformanceTest() {
  console.log('🚀 Iniciando test de rendimiento MySQL');
  console.log(`📊 Insertando ${CONFIG.totalRecords.toLocaleString()} registros en lotes de ${CONFIG.batchSize}`);
  
  let connection;
  
  try {
    connection = await mysql.createConnection({
      host: CONFIG.host,
      port: CONFIG.port,
      user: CONFIG.user,
      password: CONFIG.password
    });
    
    console.log('✅ Conectado a MySQL');
    
    await connection.query(`CREATE DATABASE IF NOT EXISTS ${CONFIG.database}`);
    await connection.query(`USE ${CONFIG.database}`);
    console.log(`📁 Base de datos '${CONFIG.database}' seleccionada`);
    
    await connection.query(CREATE_TABLE_SQL);
    console.log('📋 Tabla creada/verificada');
    
    await connection.query(`DELETE FROM ${CONFIG.tableName}`);
    await connection.query(`ALTER TABLE ${CONFIG.tableName} AUTO_INCREMENT = 1`);
    console.log('🧹 Tabla limpiada');
    
    await connection.query('SET autocommit = 0');
    await connection.query('SET unique_checks = 0');
    await connection.query('SET foreign_key_checks = 0');
    console.log('⚡ Optimizaciones aplicadas');
    
    const startTime = Date.now();
    let totalInserted = 0;
    
    for (let i = 0; i < CONFIG.totalRecords; i += CONFIG.batchSize) {
      const currentBatchSize = Math.min(CONFIG.batchSize, CONFIG.totalRecords - i);
      const inserted = await insertBatch(connection, i, currentBatchSize);
      totalInserted += inserted;
      
      if ((i / CONFIG.batchSize) % 10 === 0) {
        await connection.query('COMMIT');
      }
      
      showProgress(totalInserted, CONFIG.totalRecords, startTime);
    }
    
    await connection.query('COMMIT');
    
    await connection.query('SET autocommit = 1');
    await connection.query('SET unique_checks = 1');
    await connection.query('SET foreign_key_checks = 1');
    
    const endTime = Date.now();
    const totalTime = (endTime - startTime) / 1000;
    
    console.log('\n\n📈 RESULTADOS DEL TEST:');
    console.log('========================');
    console.log(`✅ Registros insertados: ${totalInserted.toLocaleString()}`);
    console.log(`⏱️  Tiempo total: ${totalTime.toFixed(2)} segundos`);
    console.log(`🚀 Velocidad promedio: ${(totalInserted / totalTime).toFixed(0)} docs/segundo`);
    console.log(`💾 Base de datos: ${CONFIG.database}`);
    console.log(`📦 Tabla: ${CONFIG.tableName}`);
    
    const [countResult] = await connection.query(`SELECT COUNT(*) as total FROM ${CONFIG.tableName}`);
    const actualCount = countResult[0].total;
    console.log(`✅ Verificación: ${actualCount.toLocaleString()} registros en la tabla`);
    
    const tableStats = await getTableStats(connection);
    if (tableStats) {
      console.log(`📊 Tamaño de la tabla: ${tableStats.Tamaño_MB} MB`);
      console.log(`🔧 Motor: ${tableStats.ENGINE}`);
      console.log(`📝 Collation: ${tableStats.TABLE_COLLATION}`);
    }
    
    console.log('\n🔍 EJECUTANDO CONSULTAS DE TEST:');
    console.log('================================');
    
    let queryStart = Date.now();
    const [emailResult] = await connection.query(
      `SELECT * FROM ${CONFIG.tableName} WHERE email LIKE '%@%' LIMIT 1`
    );
    let queryTime = Date.now() - queryStart;
    console.log(`📧 Búsqueda por email: ${queryTime}ms`);
    
    queryStart = Date.now();
    const [deptResult] = await connection.query(
      `SELECT COUNT(*) as total FROM ${CONFIG.tableName} WHERE departamento = 'IT'`
    );
    queryTime = Date.now() - queryStart;
    console.log(`🏢 Conteo por departamento: ${queryTime}ms (${deptResult[0].total} registros)`);
    
    queryStart = Date.now();
    const [ageResult] = await connection.query(
      `SELECT * FROM ${CONFIG.tableName} WHERE edad BETWEEN 25 AND 35 LIMIT 10`
    );
    queryTime = Date.now() - queryStart;
    console.log(`👥 Búsqueda por rango edad (25-35): ${queryTime}ms (${ageResult.length} resultados)`);
    
    queryStart = Date.now();
    const [avgResult] = await connection.query(`
      SELECT 
        departamento, 
        AVG(salario) as salario_promedio,
        COUNT(*) as total_empleados
      FROM ${CONFIG.tableName} 
      GROUP BY departamento
      ORDER BY salario_promedio DESC
    `);
    queryTime = Date.now() - queryStart;
    console.log(`💰 Agregación salario promedio: ${queryTime}ms`);
    
    queryStart = Date.now();
    const [jsonResult] = await connection.query(`
      SELECT COUNT(*) as total 
      FROM ${CONFIG.tableName} 
      WHERE JSON_CONTAINS(habilidades, '"JavaScript"')
    `);
    queryTime = Date.now() - queryStart;
    console.log(`🔧 Búsqueda en JSON (JavaScript): ${queryTime}ms (${jsonResult[0].total} registros)`);
    
    queryStart = Date.now();
    const [complexResult] = await connection.query(`
      SELECT 
        departamento,
        COUNT(*) as total,
        AVG(edad) as edad_promedio,
        MAX(salario) as salario_max
      FROM ${CONFIG.tableName} 
      WHERE activo = 1 AND edad > 25 AND salario > 50000
      GROUP BY departamento
      HAVING total > 100
      ORDER BY salario_max DESC
    `);
    queryTime = Date.now() - queryStart;
    console.log(`🔍 Consulta compleja: ${queryTime}ms (${complexResult.length} departamentos)`);
    
    console.log('\n📋 MUESTRA DE RESULTADOS:');
    console.log('=========================');
    avgResult.forEach(row => {
      console.log(`${row.departamento}: $${row.salario_promedio} (${row.total_empleados} empleados)`);
    });
    
  } catch (error) {
    console.error('❌ Error durante el test:', error.message);
    if (error.code === 'ECONNREFUSED') {
      console.error('💡 Verifica que MySQL esté ejecutándose en el puerto 3306');
    } else if (error.code === 'ER_ACCESS_DENIED_ERROR') {
      console.error('💡 Verifica las credenciales de MySQL en CONFIG');
    }
  } finally {
    if (connection) {
      await connection.end();
      console.log('\n🔌 Conexión cerrada');
    }
  }
}

function showMenu() {
  console.log('\n⚙️  CONFIGURACIÓN ACTUAL:');
  console.log(`📊 Registros: ${CONFIG.totalRecords.toLocaleString()}`);
  console.log(`📦 Tamaño de lote: ${CONFIG.batchSize}`);
  console.log(`🔗 Host: ${CONFIG.host}:${CONFIG.port}`);
  console.log(`👤 Usuario: ${CONFIG.user}`);
  console.log(`🗃️  BD: ${CONFIG.database}`);
  console.log(`📋 Tabla: ${CONFIG.tableName}`);
  console.log('\n¿Deseas continuar con esta configuración? (s/n)');
}

if (require.main === module) {
  console.log('🧪 PROGRAMA DE TESTEO MYSQL');
  console.log('===========================');
  
  try {
    require('mysql2/promise');
    require('@faker-js/faker');
  } catch (error) {
    console.error('❌ Dependencias faltantes. Ejecuta:');
    console.error('npm install mysql2 @faker-js/faker');
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
