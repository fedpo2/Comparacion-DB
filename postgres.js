const { Pool } = require('pg');
const { faker } = require('@faker-js/faker');

// Usamos CONFIG del archivo original
const CONFIG = {
  host: 'localhost',
  port: 5432,
  database: 'test_performance',
  user: 'rooto',
  password: 'tururu',
  totalRecords: 100000,
  batchSize: 1000,
  tableName: 'usuarios_test'
};

// Tabla SQL (sin cambios)
const CREATE_TABLE_SQL = `
CREATE TABLE ${CONFIG.tableName} (
  id SERIAL PRIMARY KEY,
  nombre VARCHAR(100) NOT NULL,
  apellido VARCHAR(100) NOT NULL,
  email VARCHAR(255) NOT NULL ,
  edad INTEGER NOT NULL,
  direccion_calle VARCHAR(255),
  direccion_ciudad VARCHAR(100),
  direccion_pais VARCHAR(100),
  direccion_codigo_postal VARCHAR(20),
  fecha_registro TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  activo BOOLEAN DEFAULT TRUE,
  salario DECIMAL(10,2),
  departamento VARCHAR(50),
  habilidades JSONB,
  ultima_conexion TIMESTAMP WITHOUT TIME ZONE,
  sesiones_activas INTEGER DEFAULT 0,
  puntuacion DECIMAL(3,2),
  created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
)
`;

// Función generateFakeUser (sin cambios)
function generateFakeUser(index) {
  const habilidades = faker.helpers.arrayElements(
    ['JavaScript', 'Python', 'Java', 'C++', 'React', 'Node.js', 'MySQL', 'SQL', 'Docker', 'AWS'],
    { min: 1, max: 5 }
  );

  const fechaAleatoria = faker.date.past();

  return {
    nombre: faker.person.firstName(),
    apellido: faker.person.lastName(),
    email: faker.internet.email(),
    edad: faker.number.int({ min: 18, max: 80 }),
    direccion_calle: faker.location.streetAddress(),
    direccion_ciudad: faker.location.city(),
    direccion_pais: faker.location.country(),
    direccion_codigo_postal: faker.location.zipCode(),
    fecha_registro: fechaAleatoria,
    activo: true,
    salario: parseFloat((Math.random() * 10000).toFixed(2)),
    departamento: faker.commerce.department(),
    habilidades: JSON.stringify(habilidades),
    ultima_conexion: fechaAleatoria,
    sesiones_activas: faker.number.int({ min: 0, max: 10 }),
    puntuacion: parseFloat((Math.random() * 5).toFixed(2)),
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString()
  };
}

// Corregir getTableStats para PostgreSQL
async function getTableStats(pool) {
  const res = await pool.query(`
    SELECT
      relname AS "TABLE_NAME",
      pg_total_relation_size(relid) / 1024 / 1024 AS "Tamaño_MB",
      n_live_tup AS "Filas_Estimadas"
    FROM pg_stat_user_tables
    WHERE relname = $1
  `, [CONFIG.tableName]);
  return res.rows[0];
}

let pool;

async function runPerformanceTest() {
  console.log('🚀 Iniciando test de rendimiento Postgres');
  console.log(`📊 Insertando ${CONFIG.totalRecords.toLocaleString()} registros en lotes de ${CONFIG.batchSize}`);

  try {
    pool = new Pool({
      host: CONFIG.host,
      port: CONFIG.port,
      database: CONFIG.database,
      user: CONFIG.user,
      password: CONFIG.password,
      max: 5
    });

    await pool.query("DROP TABLE usuarios_test");
    await pool.query(CREATE_TABLE_SQL);
    console.log('📋 Tabla creada/verificada');

    // Vaciar tabla antes de insertar
    await pool.query(`DELETE FROM ${CONFIG.tableName}`);
    console.log('🧹 Tabla limpiada');

    const startTime = Date.now();

    let totalInserted = 0;
    while (totalInserted < CONFIG.totalRecords) {
      const batch = [];
      for (let i = 0; i < CONFIG.batchSize && totalInserted < CONFIG.totalRecords; i++, totalInserted++) {
        const data = generateFakeUser(totalInserted);
        batch.push([
          data.nombre,
          data.apellido,
          data.email,
          data.edad,
          data.direccion_calle,
          data.direccion_ciudad,
          data.direccion_pais,
          data.direccion_codigo_postal,
          data.fecha_registro,
          data.activo,
          data.salario,
          data.departamento,
          data.habilidades,
          data.ultima_conexion,
          data.sesiones_activas,
          data.puntuacion
        ]);
      }

      const values = batch.flat();
      const placeholders = batch.map((_, idx) =>
        Array.from({ length: 16 }, (_, j) => `$${idx * 16 + j + 1}`).join(',')
      ).join('),(');

      const queryText = `
        INSERT INTO ${CONFIG.tableName} (
          nombre, apellido, email, edad, direccion_calle, direccion_ciudad,
          direccion_pais, direccion_codigo_postal, fecha_registro, activo,
          salario, departamento, habilidades, ultima_conexion, sesiones_activas, puntuacion
        ) VALUES (${placeholders})
      `;

      await pool.query(queryText, values);
    }

    const endTime = Date.now();
    const totalTime = (endTime - startTime) / 1000;
    console.log('\n📈 RESULTADOS DEL TEST:');
    console.log('========================');
    console.log(`✅ Registros insertados: ${totalInserted}`);
    console.log(`⏱️  Tiempo total: ${totalTime.toFixed(2)} segundos`);
    console.log(`🚀 Velocidad promedio: ${(totalInserted / totalTime).toFixed(2)} docs/segundo`);
    console.log(`💾 Base de datos: ${CONFIG.database}`);
    console.log(`📦 Tabla: ${CONFIG.tableName}`);

    const countResult = await pool.query(`SELECT COUNT(*) as total FROM ${CONFIG.tableName}`);
    const actualCount = countResult.rows[0].total;
    console.log(`✅ Verificación: ${actualCount.toLocaleString()} registros en la tabla`);

    const tableStats = await getTableStats(pool);
    if (tableStats) {
      console.log(`📊 Tamaño de la tabla: ${tableStats.Tamaño_MB || 0} MB`);
      console.log(`📝 Filas estimadas: ${tableStats.Filas_Estimadas}`);
    }

    console.log('\n🔍 EJECUTANDO CONSULTAS DE TEST:');
    console.log('================================');

    let queryStart = Date.now();
    await pool.query(`SELECT * FROM ${CONFIG.tableName} WHERE email LIKE '%@%' LIMIT 1`);
    let queryTime = Date.now() - queryStart;
    console.log(`📧 Búsqueda por email: ${queryTime}ms`);

    queryStart = Date.now();
    const deptResult = await pool.query(
      `SELECT COUNT(*) as total FROM ${CONFIG.tableName} WHERE departamento = 'IT'`
    );
    queryTime = Date.now() - queryStart;
    console.log(`🏢 Conteo por departamento: ${queryTime}ms (${deptResult.rows[0].total} registros)`);

    queryStart = Date.now();
    const ageResult = await pool.query(
      `SELECT * FROM ${CONFIG.tableName} WHERE edad BETWEEN 25 AND 35 LIMIT 10`
    );
    queryTime = Date.now() - queryStart;
    console.log(`👥 Búsqueda por rango edad (25-35): ${queryTime}ms (${ageResult.rows.length} resultados)`);

    queryStart = Date.now();
    const avgResult = await pool.query(`
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
    const jsonResult = await pool.query(`
      SELECT COUNT(*) as total
      FROM ${CONFIG.tableName}
      WHERE habilidades ?| ARRAY['JavaScript']
    `);
    queryTime = Date.now() - queryStart;
    console.log(`🔧 Búsqueda en JSON (JavaScript): ${queryTime}ms (${jsonResult.rows[0].total} registros)`);

    queryStart = Date.now();
    const complexResult = await pool.query(`
      SELECT
        departamento,
        COUNT(*) as total,
        AVG(edad) as edad_promedio,
        MAX(salario) as salario_max
      FROM ${CONFIG.tableName}
      WHERE activo IS TRUE AND edad > 25 AND salario > 50000
      GROUP BY departamento
      HAVING COUNT(*) > 100
      ORDER BY salario_max DESC
    `);
    queryTime = Date.now() - queryStart;
    console.log(`🔍 Consulta compleja: ${queryTime}ms (${complexResult.rows.length} departamentos)`);

    console.log('\n📋 MUESTRA DE RESULTADOS:');
    console.log('=========================');
    avgResult.rows.forEach(row => {
      console.log(`${row.departamento}: $${row.salario_promedio || 0} (${row.total_empleados} empleados)`);
    });

  } catch (error) {
    console.error('❌ Error durante el test:', error.message);
    if (error.code === 'ECONNREFUSED') {
      console.error('💡 Verifica que PostgreSQL esté ejecutándose en el puerto 5432');
    }
  } finally {
    if (pool) {
      await pool.end();
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
  console.log('🧪 PROGRAMA DE TESTEO POSTGRES');
  console.log('===========================');
  try {
    require('pg');
    require('@faker-js/faker');
  } catch (error) {
    console.error('❌ Dependencias faltantes. Ejecuta:');
    console.error('npm install pg @faker-js/faker');
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
