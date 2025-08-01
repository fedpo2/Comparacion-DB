const sql = require('mssql');
const { faker } = require('@faker-js/faker');

// Configuración de conexión a SQL Server
const CONFIG = {
  server: 'localhost',           // Cambia si tu servidor está en otra IP
  port: 1433,                    // Puerto por defecto de SQL Server
  user: 'sa',                    // Usuario (ej: sa o uno con permisos)
  password: 'Chichulina123',     // Contraseña
  database: 'test_performance',
  totalRecords: 100000,
  batchSize: 1000,               // Tamaño del lote para inserciones
  tableName: 'usuarios_test',
  options: {
    encrypt: false,              // Usa true si estás con Azure o SSL
    trustServerCertificate: true, // Útil en entornos locales/dev
    requestTimeout: 300000,      // 5 minutos timeout
    connectionTimeout: 30000     // 30 segundos para conectar
  }
};

// SQL para crear la tabla (sintaxis SQL Server corregida)
const CREATE_TABLE_SQL = `
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='${CONFIG.tableName}' AND xtype='U')
BEGIN
  CREATE TABLE ${CONFIG.tableName} (
    id INT IDENTITY(1,1) PRIMARY KEY,
    usuario_id INT NOT NULL,
    nombre NVARCHAR(100) NOT NULL,
    apellido NVARCHAR(100) NOT NULL,
    email NVARCHAR(255) NOT NULL,
    edad INT NOT NULL,
    direccion_calle NVARCHAR(255),
    direccion_ciudad NVARCHAR(100),
    direccion_pais NVARCHAR(100),
    direccion_codigo_postal NVARCHAR(20),
    fecha_registro DATETIME NOT NULL,
    activo BIT NOT NULL DEFAULT 1,
    salario DECIMAL(10,2),
    departamento NVARCHAR(50),
    habilidades NVARCHAR(MAX),
    ultima_conexion DATETIME,
    sesiones_activas INT DEFAULT 0,
    puntuacion DECIMAL(3,2),
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE()
  );

  -- Crear índices después de la tabla
  CREATE NONCLUSTERED INDEX idx_email ON ${CONFIG.tableName} (email);
  CREATE NONCLUSTERED INDEX idx_departamento ON ${CONFIG.tableName} (departamento);
  CREATE NONCLUSTERED INDEX idx_edad ON ${CONFIG.tableName} (edad);
  CREATE NONCLUSTERED INDEX idx_fecha_registro ON ${CONFIG.tableName} (fecha_registro);
  CREATE NONCLUSTERED INDEX idx_activo ON ${CONFIG.tableName} (activo);
END
`;

// Generar datos falsos con tipos estrictos
function generateFakeUser(index) {
  const habilidades = faker.helpers.arrayElements(
    ['JavaScript', 'Python', 'Java', 'C++', 'React', 'Node.js', 'MySQL', 'SQL', 'Docker', 'AWS'],
    { min: 1, max: 5 }
  );

  // Generar fecha válida reciente
  const fechaRegistro = faker.date.between({
    from: new Date(Date.now() - 365 * 24 * 60 * 60 * 1000), // hace 1 año
    to: new Date()
  });

  // Generar última conexión (puede ser null)
  const ultimaConexion = faker.datatype.boolean({ probability: 0.7 })
    ? faker.date.between({
        from: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000), // hace 30 días
        to: new Date()
      })
    : null;

  return [
    index,                                   // usuario_id → INT
    faker.person.firstName(),                // nombre → NVARCHAR(100)
    faker.person.lastName(),                 // apellido → NVARCHAR(100)
    faker.internet.email(),                  // email → NVARCHAR(255)
    faker.number.int({ min: 18, max: 80 }), // edad → INT
    faker.location.streetAddress(),          // direccion_calle → NVARCHAR(255)
    faker.location.city(),                   // direccion_ciudad → NVARCHAR(100)
    faker.location.country(),                // direccion_pais → NVARCHAR(100)
    faker.location.zipCode(),                // direccion_codigo_postal → NVARCHAR(20)
    fechaRegistro,                           // fecha_registro → DATETIME
    faker.datatype.boolean(),                // activo → BOOLEAN (se convertirá a BIT)
    faker.number.float({ min: 30000, max: 120000, precision: 0.01 }), // salario → DECIMAL(10,2)
    faker.helpers.arrayElement(['IT', 'Marketing', 'Ventas', 'RRHH', 'Finanzas']), // departamento
    JSON.stringify(habilidades),             // habilidades → NVARCHAR(MAX)
    ultimaConexion,                          // ultima_conexion → DATETIME (puede ser null)
    faker.number.int({ min: 0, max: 5 }),   // sesiones_activas → INT
    faker.number.float({ min: 1.0, max: 5.0, precision: 0.1 }) // puntuacion → DECIMAL(3,2)
  ];
}

// Insertar un lote usando mssql bulk insert (alta performance)
async function insertBatch(pool, startIndex, batchSize) {
  const table = new sql.Table(CONFIG.tableName);
  table.create = true; // Permitir que se cree automáticamente

  // Definir columnas con tipos exactos - IMPORTANTE: deben coincidir exactamente con la BD
  table.columns.add('usuario_id', sql.Int, { nullable: false });
  table.columns.add('nombre', sql.NVarChar(100), { nullable: false });
  table.columns.add('apellido', sql.NVarChar(100), { nullable: false });
  table.columns.add('email', sql.NVarChar(255), { nullable: false });
  table.columns.add('edad', sql.Int, { nullable: false });
  table.columns.add('direccion_calle', sql.NVarChar(255), { nullable: true });
  table.columns.add('direccion_ciudad', sql.NVarChar(100), { nullable: true });
  table.columns.add('direccion_pais', sql.NVarChar(100), { nullable: true });
  table.columns.add('direccion_codigo_postal', sql.NVarChar(20), { nullable: true });
  table.columns.add('fecha_registro', sql.DateTime, { nullable: false });
  table.columns.add('activo', sql.Bit, { nullable: false });
  table.columns.add('salario', sql.Decimal(10, 2), { nullable: true });
  table.columns.add('departamento', sql.NVarChar(50), { nullable: true });
  table.columns.add('habilidades', sql.NVarChar(sql.MAX), { nullable: true });
  table.columns.add('ultima_conexion', sql.DateTime, { nullable: true });
  table.columns.add('sesiones_activas', sql.Int, { nullable: true });
  table.columns.add('puntuacion', sql.Decimal(3, 2), { nullable: true });

  // Agregar filas con validación de tipos
  for (let i = startIndex; i < startIndex + batchSize; i++) {
    const userData = generateFakeUser(i);

    // Validar que los datos coincidan con los tipos esperados
    const validatedData = [
      parseInt(userData[0]) || i,                    // usuario_id - INT
      String(userData[1] || ''),                     // nombre - NVARCHAR
      String(userData[2] || ''),                     // apellido - NVARCHAR
      String(userData[3] || ''),                     // email - NVARCHAR
      parseInt(userData[4]) || 25,                   // edad - INT
      userData[5] ? String(userData[5]) : null,      // direccion_calle - NVARCHAR nullable
      userData[6] ? String(userData[6]) : null,      // direccion_ciudad - NVARCHAR nullable
      userData[7] ? String(userData[7]) : null,      // direccion_pais - NVARCHAR nullable
      userData[8] ? String(userData[8]) : null,      // direccion_codigo_postal - NVARCHAR nullable
      userData[9] instanceof Date ? userData[9] : new Date(), // fecha_registro - DATETIME
      Boolean(userData[10]) ? 1 : 0,                 // activo - BIT (1 o 0)
      userData[11] ? parseFloat(userData[11]) : null, // salario - DECIMAL nullable
      userData[12] ? String(userData[12]) : null,    // departamento - NVARCHAR nullable
      userData[13] ? String(userData[13]) : null,    // habilidades - NVARCHAR(MAX) nullable
      userData[14] instanceof Date ? userData[14] : null, // ultima_conexion - DATETIME nullable
      userData[15] ? parseInt(userData[15]) : 0,     // sesiones_activas - INT nullable
      userData[16] ? parseFloat(userData[16]) : null // puntuacion - DECIMAL nullable
    ];

    table.rows.add(...validatedData);
  }

  try {
    const request = pool.request();
    const result = await request.bulk(table);
    return result.rowsAffected;
  } catch (error) {
    console.error(`\n❌ Error en lote ${startIndex}: ${error.message}`);
    // Log del primer registro para debug
    if (table.rows.length > 0) {
      console.error('📋 Primer registro del lote:', table.rows[0]);
    }
    throw error;
  }
}

// Mostrar progreso en tiempo real
function showProgress(inserted, total, startTime) {
  const elapsed = (Date.now() - startTime) / 1000;
  const rate = inserted / elapsed;
  const remaining = total - inserted;
  const eta = remaining / rate;

  if (process.stdout.isTTY) {
    process.stdout.clearLine();
    process.stdout.cursorTo(0);
    process.stdout.write(
      `Progreso: ${inserted}/${total} (${((inserted/total)*100).toFixed(1)}%) | ` +
      `Velocidad: ${rate.toFixed(0)} docs/s | ` +
      `ETA: ${eta.toFixed(0)}s`
    );
  } else {
    // Para entornos que no soportan TTY
    if (inserted % (CONFIG.batchSize * 10) === 0) {
      console.log(`Progreso: ${inserted}/${total} (${((inserted/total)*100).toFixed(1)}%)`);
    }
  }
}

// Ejecutar el test de rendimiento
async function runPerformanceTest() {
  console.log('🚀 Iniciando test de rendimiento SQL Server');
  console.log(`📊 Insertando ${CONFIG.totalRecords.toLocaleString()} registros en lotes de ${CONFIG.batchSize}`);

  let pool;
  try {
    // Configuración de conexión completa
    const config = {
      user: CONFIG.user,
      password: CONFIG.password,
      server: CONFIG.server,
      port: CONFIG.port,
      database: 'master', // Conectar primero a master para crear BD
      options: CONFIG.options
    };

    // Conexión inicial a master
    pool = await sql.connect(config);
    console.log('✅ Conectado a SQL Server (master)');

    // Crear base de datos si no existe
    try {
      await pool.request().query(`
        IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '${CONFIG.database}')
        CREATE DATABASE ${CONFIG.database}
      `);
      console.log(`📁 Base de datos '${CONFIG.database}' verificada`);
    } catch (dbError) {
      console.log(`⚠️  Error creando BD (puede que ya exista): ${dbError.message}`);
    }

    // Cerrar conexión a master y reconectar a la BD objetivo
    await pool.close();

    config.database = CONFIG.database;
    pool = await sql.connect(config);
    console.log(`✅ Conectado a base de datos '${CONFIG.database}'`);

    // Crear tabla e índices
    try {
      await pool.request().query(CREATE_TABLE_SQL);
      console.log('📋 Tabla e índices creados/verificados');
    } catch (tableError) {
      console.log(`⚠️  Error con tabla: ${tableError.message}`);
      // Continuar si la tabla ya existe
    }

    // Limpiar tabla
    try {
      await pool.request().query(`DELETE FROM ${CONFIG.tableName}`);
      await pool.request().query(`DBCC CHECKIDENT ('${CONFIG.tableName}', RESEED, 0)`);
      console.log('🧹 Tabla limpiada');
    } catch (cleanError) {
      console.log(`⚠️  Error limpiando tabla: ${cleanError.message}`);
    }

    const startTime = Date.now();
    let totalInserted = 0;

    // Insertar datos en lotes
    for (let i = 0; i < CONFIG.totalRecords; i += CONFIG.batchSize) {
      const currentBatchSize = Math.min(CONFIG.batchSize, CONFIG.totalRecords - i);

      try {
        const inserted = await insertBatch(pool, i + 1, currentBatchSize); // +1 para empezar en 1
        totalInserted += inserted;
        showProgress(totalInserted, CONFIG.totalRecords, startTime);
      } catch (batchError) {
        console.error(`\n❌ Error en lote ${i}: ${batchError.message}`);
        break; // Salir del loop si hay error crítico
      }
    }

    const endTime = Date.now();
    const totalTime = (endTime - startTime) / 1000;

    console.log('\n\n📈 RESULTADOS DEL TEST:');
    console.log('========================');
    console.log(`✅ Registros insertados: ${totalInserted.toLocaleString()}`);
    console.log(`⏱️  Tiempo total: ${totalTime.toFixed(2)} segundos`);
    console.log(`🚀 Velocidad promedio: ${(totalInserted / totalTime).toFixed(0)} docs/segundo`);
    console.log(`💾 Base de datos: ${CONFIG.database}`);
    console.log(`📦 Tabla: ${CONFIG.tableName}`);

    // Verificación de conteo
    try {
      const result = await pool.request().query(`SELECT COUNT(*) as total FROM ${CONFIG.tableName}`);
      const actualCount = result.recordset[0].total;
      console.log(`✅ Verificación: ${actualCount.toLocaleString()} registros en la tabla`);
    } catch (countError) {
      console.log(`⚠️  Error verificando conteo: ${countError.message}`);
    }

    // Consultas de prueba
    console.log('\n🔍 EJECUTANDO CONSULTAS DE TEST:');
    console.log('================================');

    const queries = [
      {
        name: '📧 Búsqueda por email',
        sql: `SELECT TOP 1 * FROM ${CONFIG.tableName} WHERE email LIKE '%@gmail.com%'`
      },
      {
        name: '🏢 Conteo por departamento',
        sql: `SELECT departamento, COUNT(*) as total FROM ${CONFIG.tableName} WHERE departamento = 'IT' GROUP BY departamento`
      },
      {
        name: '👥 Búsqueda por rango edad (25-35)',
        sql: `SELECT TOP 10 * FROM ${CONFIG.tableName} WHERE edad BETWEEN 25 AND 35`
      },
      {
        name: '💰 Agregación salario promedio',
        sql: `
          SELECT
            departamento,
            AVG(CAST(salario AS FLOAT)) as salario_promedio,
            COUNT(*) as total_empleados
          FROM ${CONFIG.tableName}
          GROUP BY departamento
          ORDER BY salario_promedio DESC
        `
      },
      {
        name: '🔧 Búsqueda en JSON (JavaScript)',
        sql: `
          SELECT COUNT(*) as total
          FROM ${CONFIG.tableName}
          WHERE habilidades LIKE '%JavaScript%'
        `
      },
      {
        name: '🔍 Consulta compleja',
        sql: `
          SELECT
            departamento,
            COUNT(*) as total,
            AVG(CAST(edad AS FLOAT)) as edad_promedio,
            MAX(salario) as salario_max
          FROM ${CONFIG.tableName}
          WHERE activo = 1 AND edad > 25 AND salario > 50000
          GROUP BY departamento
          HAVING COUNT(*) > 10
          ORDER BY salario_max DESC
        `
      }
    ];

    for (const query of queries) {
      try {
        const start = Date.now();
        const result = await pool.request().query(query.sql);
        const time = Date.now() - start;
        console.log(`${query.name}: ${time}ms (${result.recordset.length} resultados)`);
      } catch (queryError) {
        console.log(`${query.name}: ❌ Error - ${queryError.message}`);
      }
    }

  } catch (error) {
    console.error('\n❌ Error durante el test:', error.message);

    // Mensajes de ayuda específicos
    if (error.code === 'ELOGIN') {
      console.error('💡 Error de login. Verifica usuario/contraseña en CONFIG.');
    } else if (error.code === 'ESOCKET') {
      console.error('💡 No se pudo conectar al servidor. ¿Está SQL Server corriendo?');
      console.error('💡 Verifica que el puerto 1433 esté abierto y accesible.');
    } else if (error.code === 'ETIMEOUT') {
      console.error('💡 Timeout de conexión. El servidor puede estar sobrecargado.');
    } else {
      console.error('📋 Código de error:', error.code);
      console.error('📋 Detalles:', error.originalError?.message || error.message);
    }
  } finally {
    if (pool) {
      try {
        await pool.close();
        console.log('\n🔌 Conexión cerrada');
      } catch (closeError) {
        console.error('⚠️  Error cerrando conexión:', closeError.message);
      }
    }
  }
}

// Menú inicial
function showMenu() {
  console.log('\n⚙️  CONFIGURACIÓN ACTUAL:');
  console.log('========================');
  console.log(`📊 Registros: ${CONFIG.totalRecords.toLocaleString()}`);
  console.log(`📦 Tamaño de lote: ${CONFIG.batchSize}`);
  console.log(`🔗 Servidor: ${CONFIG.server}:${CONFIG.port}`);
  console.log(`👤 Usuario: ${CONFIG.user}`);
  console.log(`🗃️  Base de datos: ${CONFIG.database}`);
  console.log(`📋 Tabla: ${CONFIG.tableName}`);
  console.log('\n¿Deseas continuar con esta configuración? (s/n): ');
}

// Validar dependencias
function checkDependencies() {
  try {
    require('mssql');
    require('@faker-js/faker');
    return true;
  } catch (error) {
    console.error('❌ Dependencias faltantes. Ejecuta:');
    console.error('npm install mssql @faker-js/faker');
    return false;
  }
}

// Ejecución principal
if (require.main === module) {
  console.log('🧪 PROGRAMA DE TESTEO SQL SERVER');
  console.log('================================');

  if (!checkDependencies()) {
    process.exit(1);
  }

  showMenu();

  const readline = require('readline');
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });

  rl.question('', (answer) => {
    rl.close();

    if (['s', 'si', 'y', 'yes', ''].includes(answer.toLowerCase().trim())) {
      runPerformanceTest().catch((error) => {
        console.error('\n💥 Error fatal:', error.message);
        process.exit(1);
      });
    } else {
      console.log('❌ Test cancelado');
      process.exit(0);
    }
  });
}

module.exports = { runPerformanceTest, generateFakeUser, CONFIG };
