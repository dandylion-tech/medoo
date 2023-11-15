<?php

declare(strict_types=1);
/**
 * Medoo Database Framework.
 *
 * The Lightweight PHP Database Framework to Accelerate Development.
 *
 * @version 2.1.10
 * @author Angel Lai
 * @package Medoo
 * @copyright Copyright 2023 Medoo Project, Angel Lai.
 * @license https://opensource.org/licenses/MIT
 * @link https://medoo.in
 */

namespace Dandylion;

use PDO;
use Exception;
use PDOException;
use PDOStatement;
use InvalidArgumentException;

/**
 * The Medoo raw object.
 */
class Raw
{
    /**
     * The array of mapping data for the raw string.
     *
     * @var array
     */
    public $map;

    /**
     * The raw string.
     *
     * @var string
     */
    public $value;
}

/**
 * @method array select(string $table, array $columns, array $where)
 * @method null select(string $table, array $columns, callable $callback)
 * @method null select(string $table, array $columns, array $where, callable $callback)
 * @method null select(string $table, array $join, array $columns, array $where, callable $callback)
 * @method mixed get(string $table, array|string $columns, array $where)
 * @method bool has(string $table, array $where)
 * @method mixed rand(string $table, array|string $column, array $where)
 * @method int count(string $table, array $where)
 * @method string max(string $table, string $column)
 * @method string min(string $table, string $column)
 * @method string avg(string $table, string $column)
 * @method string sum(string $table, string $column)
 * @method string max(string $table, string $column, array $where)
 * @method string min(string $table, string $column, array $where)
 * @method string avg(string $table, string $column, array $where)
 * @method string sum(string $table, string $column, array $where)
 */
class Medoo
{
    /**
     * The PDO object.
     *
     * @var \PDO
     */
    public $pdo;

    /**
     * The type of database.
     *
     * @var string
     */
    public $type;

    /**
     * Table prefix.
     *
     * @var string
     */
    protected $prefix;

    /**
     * The PDO statement object.
     *
     * @var \PDOStatement
     */
    protected $statement;

    /**
     * The DSN connection string.
     *
     * @var string
     */
    protected $dsn;

    /**
     * The array of logs.
     *
     * @var array
     */
    protected $logs = [];

    /**
     * Determine should log the query or not.
     *
     * @var bool
     */
    protected $logging = false;

    /**
     * Determine is in test mode.
     *
     * @var bool
     */
    protected $testMode = false;

    /**
     * The last query string was generated in test mode.
     *
     * @var string
     */
    public $queryString;

    /**
     * Determine is in debug mode.
     *
     * @var bool
     */
    protected $debugMode = false;

    /**
     * Determine should save debug logging.
     *
     * @var bool
     */
    protected $debugLogging = false;

    /**
     * The array of logs for debugging.
     *
     * @var array
     */
    protected $debugLogs = [];

    /**
     * The unique global id.
     *
     * @var integer
     */
    protected $guid = 0;

    /**
     * The returned id for the insert.
     *
     * @var string
     */
    public $returnId = '';

    /**
     * Error Message.
     *
     * @var string|null
     */
    public $error = null;

    /**
     * The array of error information.
     *
     * @var array|null
     */
    public $errorInfo = null;

    /**
     * Connect the database.
     *
     * ```
     * $database = new Medoo([
     *      // required
     *      'type' => 'mysql',
     *      'database' => 'name',
     *      'host' => 'localhost',
     *      'username' => 'your_username',
     *      'password' => 'your_password',
     *
     *      // [optional]
     *      'charset' => 'utf8mb4',
     *      'port' => 3306,
     *      'prefix' => 'PREFIX_'
     * ]);
     * ```
     *
     * @param array $options Connection options
     * @return Medoo
     * @throws PDOException
     * @link https://medoo.in/api/new
     * @codeCoverageIgnore
     */
	protected $schema_quotes;
    protected $table_quotes;
    protected $column_quotes;
    protected $quotes;

    public function __construct(array $options)
    {
        $this->quotes = $options['quotes'] ?? '"';
        $this->column_quotes = $options['column_quotes'] ?? $this->quotes;
        $this->table_quotes = $options['table_quotes'] ?? $this->quotes;
		$this->schema_quotes = $options['schema_quotes'] ?? $this->table_quotes;
        if (isset($options['prefix'])) {
            $this->prefix = $options['prefix'];
        }

        if (isset($options['testMode']) && $options['testMode'] == true) {
            $this->testMode = true;
            return;
        }

        $options['type'] = $options['type'] ?? $options['database_type'];

        if (!isset($options['pdo'])) {
            $options['database'] = $options['database'] ?? $options['database_name'];

            if (!isset($options['socket'])) {
                $options['host'] = $options['host'] ?? $options['server'] ?? false;
            }
        }

        if (isset($options['type'])) {
            $this->type = strtolower($options['type']);

            if ($this->type === 'mariadb') {
                $this->type = 'mysql';
            }
        }

        if (isset($options['logging']) && is_bool($options['logging'])) {
            $this->logging = $options['logging'];
        }

        $option = $options['option'] ?? [];
        $commands = [];

        switch ($this->type) {

            case 'mysql':
                // Make MySQL using standard quoted identifier.
                $commands[] = 'SET SQL_MODE=ANSI_QUOTES';

                break;

            case 'mssql':
                // Keep MSSQL QUOTED_IDENTIFIER is ON for standard quoting.
                $commands[] = 'SET QUOTED_IDENTIFIER ON';

                // Make ANSI_NULLS is ON for NULL value.
                $commands[] = 'SET ANSI_NULLS ON';

                break;
        }

        if (isset($options['pdo'])) {
            if (!$options['pdo'] instanceof PDO) {
                throw new InvalidArgumentException('Invalid PDO object supplied.');
            }

            $this->pdo = $options['pdo'];

            foreach ($commands as $value) {
                $this->pdo->exec($value);
            }

            return;
        }

        if (isset($options['dsn'])) {
            if (is_array($options['dsn']) && isset($options['dsn']['driver'])) {
                $attr = $options['dsn'];
            } else {
                throw new InvalidArgumentException('Invalid DSN option supplied.');
            }
        } else {
            if (
                isset($options['port']) &&
                is_int($options['port'] * 1)
            ) {
                $port = $options['port'];
            }

            $isPort = isset($port);

            switch ($this->type) {

                case 'mysql':
                    $attr = [
                        'driver' => 'mysql',
                        'dbname' => $options['database']
                    ];

                    if (isset($options['socket'])) {
                        $attr['unix_socket'] = $options['socket'];
                    } else {
                        $attr['host'] = $options['host'];

                        if ($isPort) {
                            $attr['port'] = $port;
                        }
                    }

                    break;

                case 'pgsql':
                    $attr = [
                        'driver' => 'pgsql',
                        'host' => $options['host'],
                        'dbname' => $options['database']
                    ];

                    if ($isPort) {
                        $attr['port'] = $port;
                    }

                    break;

                case 'sybase':
                    $attr = [
                        'driver' => 'dblib',
                        'host' => $options['host'],
                        'dbname' => $options['database']
                    ];

                    if ($isPort) {
                        $attr['port'] = $port;
                    }

                    break;

                case 'oracle':
                    $attr = [
                        'driver' => 'oci',
                        'dbname' => $options['host'] ?
                            '//' . $options['host'] . ($isPort ? ':' . $port : ':1521') . '/' . $options['database'] :
                            $options['database']
                    ];

                    if (isset($options['charset'])) {
                        $attr['charset'] = $options['charset'];
                    }

                    break;

                case 'mssql':
                    if (isset($options['driver']) && $options['driver'] === 'dblib') {
                        $attr = [
                            'driver' => 'dblib',
                            'host' => $options['host'] . ($isPort ? ':' . $port : ''),
                            'dbname' => $options['database']
                        ];

                        if (isset($options['appname'])) {
                            $attr['appname'] = $options['appname'];
                        }

                        if (isset($options['charset'])) {
                            $attr['charset'] = $options['charset'];
                        }
                    } else {
                        $attr = [
                            'driver' => 'sqlsrv',
                            'Server' => $options['host'] . ($isPort ? ',' . $port : ''),
                            'Database' => $options['database']
                        ];

                        if (isset($options['appname'])) {
                            $attr['APP'] = $options['appname'];
                        }

                        $config = [
                            'ApplicationIntent',
                            'AttachDBFileName',
                            'Authentication',
                            'ColumnEncryption',
                            'ConnectionPooling',
                            'Encrypt',
                            'Failover_Partner',
                            'KeyStoreAuthentication',
                            'KeyStorePrincipalId',
                            'KeyStoreSecret',
                            'LoginTimeout',
                            'MultipleActiveResultSets',
                            'MultiSubnetFailover',
                            'Scrollable',
                            'TraceFile',
                            'TraceOn',
                            'TransactionIsolation',
                            'TransparentNetworkIPResolution',
                            'TrustServerCertificate',
                            'WSID',
                        ];

                        foreach ($config as $value) {
                            $keyname = strtolower(preg_replace(['/([a-z\d])([A-Z])/', '/([^_])([A-Z][a-z])/'], '$1_$2', $value));

                            if (isset($options[$keyname])) {
                                $attr[$value] = $options[$keyname];
                            }
                        }
                    }

                    break;

                case 'sqlite':
                    $attr = [
                        'driver' => 'sqlite',
                        $options['database']
                    ];

                    break;
            }
        }

        if (!isset($attr)) {
            throw new InvalidArgumentException('Incorrect connection options.');
        }

        $driver = $attr['driver'];

        if (!in_array($driver, PDO::getAvailableDrivers())) {
            throw new InvalidArgumentException("Unsupported PDO driver: {$driver}.");
        }

        unset($attr['driver']);

        $stack = [];

        foreach ($attr as $key => $value) {
            $stack[] = is_int($key) ? $value : $key . '=' . $value;
        }

        $dsn = $driver . ':' . implode(';', $stack);

        if (
            in_array($this->type, ['mysql', 'pgsql', 'sybase', 'mssql']) &&
            isset($options['charset'])
        ) {
            $commands[] = "SET NAMES '{$options['charset']}'" . (
                $this->type === 'mysql' && isset($options['collation']) ?
                " COLLATE '{$options['collation']}'" : ''
            );
        }

        $this->dsn = $dsn;

        try {
            $this->pdo = new PDO(
                $dsn,
                $options['username'] ?? null,
                $options['password'] ?? null,
                $option
            );

            if (isset($options['error'])) {
                $this->pdo->setAttribute(
                    PDO::ATTR_ERRMODE,
                    in_array($options['error'], [
                        PDO::ERRMODE_SILENT,
                        PDO::ERRMODE_WARNING,
                        PDO::ERRMODE_EXCEPTION
                    ]) ?
                    $options['error'] :
                    PDO::ERRMODE_SILENT
                );
            }

            if (isset($options['command']) && is_array($options['command'])) {
                $commands = array_merge($commands, $options['command']);
            }

            foreach ($commands as $value) {
                $this->pdo->exec($value);
            }
        } catch (PDOException $e) {
            throw new PDOException($e->getMessage());
        }
    }

    /**
     * Generate a new map key for the placeholder.
     *
     * @return string
     */
    protected function mapKey(): string
    {
        return ':MeD' . $this->guid++ . '_mK';
    }

    /**
     * Execute customized raw statement.
     *
     * @param string $statement The raw SQL statement.
     * @param array $map The array of input parameters value for prepared statement.
     * @return \PDOStatement|null
     */
    public function query(string $statement, array $map = []): ?PDOStatement
    {
        $raw = $this->raw($statement, $map);
        $statement = $this->buildRaw($raw, $map);

        return $this->exec($statement, $map);
    }

    /**
     * Execute the raw statement.
     *
     * @param string $statement The SQL statement.
     * @param array $map The array of input parameters value for prepared statement.
     * @codeCoverageIgnore
     * @return \PDOStatement|null
     */
    public function exec(string $statement, array $map = [], callable $callback = null): ?PDOStatement
    {
        $this->statement = null;
        $this->errorInfo = null;
        $this->error = null;

        if ($this->testMode) {
            $this->queryString = $this->generate($statement, $map);
            return null;
        }

        if ($this->debugMode) {
            if ($this->debugLogging) {
                $this->debugLogs[] = $this->generate($statement, $map);
                return null;
            }

            echo $this->generate($statement, $map);

            $this->debugMode = false;

            return null;
        }

        if ($this->logging) {
            $this->logs[] = [$statement, $map];
        } else {
            $this->logs = [[$statement, $map]];
        }

        $statement = $this->pdo->prepare($statement);
        $errorInfo = $this->pdo->errorInfo();

        if ($errorInfo[0] !== '00000') {
            $this->errorInfo = $errorInfo;
            $this->error = $errorInfo[2];

            return null;
        }

        foreach ($map as $key => $value) {
            $statement->bindValue($key, $value[0], $value[1]);
        }

        if (is_callable($callback)) {
            $this->pdo->beginTransaction();
            $callback($statement);
            $execute = $statement->execute();
            $this->pdo->commit();
        } else {
            $execute = $statement->execute();
        }

        $errorInfo = $statement->errorInfo();

        if ($errorInfo[0] !== '00000') {
            $this->errorInfo = $errorInfo;
            $this->error = $errorInfo[2];

            return null;
        }

        if ($execute) {
            $this->statement = $statement;
        }

        return $statement;
    }

    /**
     * Generate readable statement.
     *
     * @param string $statement
     * @param array $map
     * @codeCoverageIgnore
     * @return string
     */
    protected function generate(string $statement, array $map): string
    {
        $identifier = [
            'mysql' => '`$1`',
            'mssql' => '[$1]'
        ];

        $statement = preg_replace(
            '/(?!\'[^\s]+\s?)"([\p{L}_][\p{L}\p{N}@$#\-_]*)"(?!\s?[^\s]+\')/u',
            $identifier[$this->type] ?? '"$1"',
            $statement
        );

        foreach ($map as $key => $value) {
            if ($value[1] === PDO::PARAM_STR) {
                $replace = $this->quote($value[0]);
            } elseif ($value[1] === PDO::PARAM_NULL) {
                $replace = 'NULL';
            } elseif ($value[1] === PDO::PARAM_LOB) {
                $replace = '{LOB_DATA}';
            } else {
                $replace = $value[0] . '';
            }

            $statement = str_replace($key, $replace, $statement);
        }

        return $statement;
    }

    /**
     * Build a raw object.
     *
     * @param string $string The raw string.
     * @param array $map The array of mapping data for the raw string.
     * @return Medoo::raw
     */
    public static function raw(string $string, array $map = []): Raw
    {
        $raw = new Raw();

        $raw->map = $map;
        $raw->value = $string;

        return $raw;
    }

    /**
     * Finds whether the object is raw.
     *
     * @param object $object
     * @return bool
     */
    protected function isRaw($object): bool
    {
        return $object instanceof Raw;
    }

    /**
     * Generate the actual query from the raw object.
     *
     * @param mixed $raw
     * @param array $map
     * @return string|null
     */
    protected function buildRaw($raw, array &$map): ?string
    {
        if (!$this->isRaw($raw)) {
            return null;
        }

        $query = preg_replace_callback(
            '/(([`\']).*?)?((FROM|TABLE|INTO|UPDATE|JOIN|TABLE IF EXISTS)\s*)?\<(((?:[\p{L}_][\p{L}\p{N}@$#\-_]*\.)?[\p{L}_][\p{L}\p{N}@$#\-_]*)(\.[\p{L}_][\p{L}\p{N}@$#\-_]*)?)\>([^,]*?\2)?/u',
            function ($matches) {
                if (!empty($matches[2]) && isset($matches[8])) {
                    return $matches[0];
                }

                if (!empty($matches[4])) {
					$table_schema = explode(".",$matches[5]);
					$table = $this->tableQuote(end($table_schema));
					if(count($table_schema)==2){
						$table = $this->schemaQuote($table_schema[0]).".".$table;
					}
                    return $matches[1] . $matches[4] . ' ' . $table;
                }

                return $matches[1] . $this->columnQuote($matches[5]);
            },
            $raw->value
        );

        $rawMap = $raw->map;

        if (!empty($rawMap)) {
            foreach ($rawMap as $key => $value) {
                $map[$key] = $this->typeMap($value, gettype($value));
            }
        }

        return $query;
    }

    /**
     * Quote a string for use in a query.
     *
     * @param string $string
     * @return string
     */
    public function quote(string $string): string
    {
        if ($this->type === 'mysql') {
            return "'" . preg_replace(['/([\'"])/', '/(\\\\\\\")/'], ["\\\\\${1}", '\\\${1}'], $string) . "'";
        }

        return "'" . preg_replace('/\'/', '\'\'', $string) . "'";
    }

    /**
     * Quote schema name for use in a query.
     *
     * @param string $schema
     * @return string
     */
    public function schemaQuote(string $schema): string
    {
        if (preg_match('/^[\p{L}_][\p{L}\p{N}@$#\-_]*$/u', $schema)) {
            return $this->schema_quotes . $this->prefix . $schema . $this->schema_quotes;
        }

        throw new InvalidArgumentException("Incorrect schema name: {$schema}.");
    }

    /**
     * Quote table name for use in a query.
     *
     * @param string $table
     * @return string
     */
    public function tableQuote(string $table): string
    {
        if (preg_match('/^[\p{L}_][\p{L}\p{N}@$#\-_]*$/u', $table)) {
            return $this->table_quotes . $this->prefix . $table . $this->table_quotes;
        }

        throw new InvalidArgumentException("Incorrect table name: {$table}.");
    }

    /**
     * Quote column name for use in a query.
     *
     * @param string $column
     * @return string
     */
    public function columnQuote(string $column): string
    {
        if (preg_match('/^[\p{L}_][\p{L}\p{N}@$#\-_]*(\.?[\p{L}_][\p{L}\p{N}@$#\-_]*)?$/u', $column)) {
            return strpos($column, '.') !== false ?
            $this->column_quotes . $this->prefix . str_replace('.', $this->column_quotes.'.'.$this->column_quotes, $column) . $this->column_quotes :
            $this->column_quotes . $column . $this->column_quotes;
        }

        throw new InvalidArgumentException("Incorrect column name: {$column}.");
    }

    /**
     * Mapping the type name as PDO data type.
     *
     * @param mixed $value
     * @param string $type
     * @return array
     */
    protected function typeMap($value, string $type): array
    {
        $map = [
            'NULL' => PDO::PARAM_NULL,
            'integer' => PDO::PARAM_INT,
            'double' => PDO::PARAM_STR,
            'boolean' => PDO::PARAM_BOOL,
            'string' => PDO::PARAM_STR,
            'object' => PDO::PARAM_STR,
            'resource' => PDO::PARAM_LOB
        ];

        if ($type === 'boolean') {
            $value = ($value ? '1' : '0');
        } elseif ($type === 'NULL') {
            $value = null;
        }

        return [$value, $map[$type]];
    }

    /**
     * Build the statement part for the column stack.
     *
     * @param array|string $columns
     * @param array $map
     * @param bool $root
     * @param bool $isJoin
     * @return string
     */
    protected function columnPush(&$columns, array &$map, bool $root, bool $isJoin = false): string
    {
        if ($columns === '*') {
            return $columns;
        }

        $stack = [];
        $hasDistinct = false;

        if (is_string($columns)) {
            $columns = [$columns];
        }
        foreach ($columns as $key => $value) {
            $isIntKey = is_int($key);
            $isArrayValue = is_array($value);

            if (!$isIntKey && $isArrayValue && $root && count(array_keys($columns)) === 1) {
				$table_column = explode(".",$key);
				$column = (end($table_column)=="*"?"*":$this->columnQuote(end($table_column)));
				if(count($table_column)==2){
					$column = $this->tableQuote($table_column[0]).".".$column;
				}
                $stack[] = $column;
                $stack[] = $this->columnPush($value, $map, false, $isJoin);
            } elseif ($isArrayValue) {
                $stack[] = $this->columnPush($value, $map, false, $isJoin);
            } elseif (!$isIntKey && $raw = $this->buildRaw($value, $map)) {
                preg_match('/(?:(?<table>[\p{L}_][\p{L}\p{N}@$#\-_]*)\.)?(?<column>[\p{L}_*][\p{L}\p{N}@$#\-_]*)(\s*\[(?<type>(String|Bool|Int|Number))\])?/u', $key, $match);
                $stack[] = "{$raw} AS ".($match["table"]?$this->tableQuote($match["table"]).".":"").($match['column']=="*"?"*":$this->columnQuote($match['column']));
            } elseif ($isIntKey && is_string($value)) {
                /*if ($isJoin && strpos($value, '*') !== false) {
                    throw new InvalidArgumentException('Cannot use table.* to select all columns while joining table.');
                }*/

                preg_match('/(?:(?<table>[\p{L}_][\p{L}\p{N}@$#\-_]*)\.)?(?<column>[\p{L}_*][\p{L}\p{N}@$#\-_]*)(?:\s*\((?<alias>[\p{L}_][\p{L}\p{N}@$#\-_]*)\))?(?:\s*\[(?<type>(?:String|Bool|Int|Number|Object|JSON))\])?/u', $value, $match);

                $columnString = '';
				if($match["column"]=="*"){
					$column_list = $this->query("SELECT column_name FROM information_schema.columns WHERE table_name = :table",[":table"=>$match['table']])->fetchAll(\PDO::FETCH_COLUMN);
				} else {
					$column_list = [$match["column"]];
				}
				$column_name_list = [];
				foreach($column_list as $column_name){
					if (!empty($match['alias'])) {
						$columnString = ($match['table']?$this->tableQuote($match['table']).".":"").($this->columnQuote($column_name))." AS ".$this->columnQuote($match['alias'].($match["column"]=="*"?$column_name:""));
						
						if(is_int($key)){
							$column_name_list[] = $match['alias'].($match["column"]=="*"?$column_name:"");
						} else {
							$columns[$key] = $match['alias'].($match["column"]=="*"?$column_name:"");
						}
						if (!empty($match['type'])) {
							if(is_int($key)){
								$column_name_list[] =$columns[$key] .= ' [' . $match['type'] . ']';
							} else {
								$columns[$key] .= ' [' . $match['type'] . ']';
							}
						}
					} else {
						$columnString = ($match["table"]?$this->tableQuote($match["table"]).".":"").($match['column']=="*"?"*":$this->columnQuote($match['column']));
					}

					if (!$hasDistinct && strpos($value, '@') === 0) {
						$columnString = 'DISTINCT ' . $columnString;
						$hasDistinct = true;
						array_unshift($stack, $columnString);

						continue;
					}

					$stack[] = $columnString;
				}
				if(count($column_name_list)>0){
					array_splice($columns,array_search($value,$columns),1,$column_name_list);
				}
            }
        }

        return implode(',', $stack);
    }

    /**
     * Implode the Where conditions.
     *
     * @param array $data
     * @param array $map
     * @param string $conjunctor
     * @return string
     */
    protected function dataImplode(array $data, array &$map, string $conjunctor): string
    {
        $stack = [];

        foreach ($data as $key => $value) {
            $type = gettype($value);

            if (
                $type === 'array' &&
                preg_match("/^(AND|OR)(\s+#.*)?$/", $key, $relationMatch)
            ) {
                $stack[] = '(' . $this->dataImplode($value, $map, ' ' . $relationMatch[1]) . ')';
                continue;
            }

            $mapKey = $this->mapKey();
            $isIndex = is_int($key);
			$isRaw = $this->isRaw($value);
			if($isIndex&&$isRaw){
				$stack[] = $this->buildRaw($value,$map);
				continue;
			}
			if(preg_match("/([A-Z_]+\(.*\))(\[(?<operator>.*)\])?([\p{L}_][\p{L}\p{N}@$#\-_\.]*)?/u",$key,$match)){
				$column = $match[1];
			} else {
				preg_match(
					'/([\p{L}_][\p{L}\p{N}@$#\-_\.]*)(\[(?<operator>.*)\])?([\p{L}_][\p{L}\p{N}@$#\-_\.]*)?/u',
					$isIndex ? $value : $key,
					$match
				);

				$column = $this->columnQuote($match[1]);
			}
			$operator = $match['operator'] ?? null;

			if ($isIndex && isset($match[4]) && in_array($operator, ['>', '>=', '<', '<=', '=', '!='])) {
				$stack[] = "{$column} {$operator} " . $this->columnQuote($match[4]);
				continue;
			}

            if ($operator && $operator != '=') {
                if (in_array($operator, ['>', '>=', '<', '<='])) {
                    $condition = "{$column} {$operator} ";

                    if (is_numeric($value)) {
                        $condition .= $mapKey;
                        $map[$mapKey] = [$value, is_float($value) ? PDO::PARAM_STR : PDO::PARAM_INT];
                    } elseif ($raw = $this->buildRaw($value, $map)) {
                        $condition .= $raw;
                    } else {
                        $condition .= $mapKey;
                        $map[$mapKey] = [$value, PDO::PARAM_STR];
                    }

                    $stack[] = $condition;
                } elseif ($operator === '!') {
                    switch ($type) {

                        case 'NULL':
                            $stack[] = $column . ' IS NOT NULL';
                            break;

                        case 'array':
                            $placeholders = [];

                            foreach ($value as $index => $item) {
                                $stackKey = $mapKey . $index . '_i';
                                $placeholders[] = $stackKey;
                                $map[$stackKey] = $this->typeMap($item, gettype($item));
                            }

                            $stack[] = $column . ' NOT IN (' . implode(', ', $placeholders) . ')';
                            break;

                        case 'object':
                            if ($raw = $this->buildRaw($value, $map)) {
                                $stack[] = "{$column} != {$raw}";
                            }
                            break;

                        case 'integer':
                        case 'double':
                        case 'boolean':
                        case 'string':
                            $stack[] = "{$column} != {$mapKey}";
                            $map[$mapKey] = $this->typeMap($value, $type);
                            break;
                    }
                } elseif ($operator === '~' || $operator === '!~') {
                    if ($type !== 'array') {
                        $value = [$value];
                    }

                    $connector = ' OR ';
                    $data = array_values($value);

                    if (is_array($data[0])) {
                        if (isset($value['AND']) || isset($value['OR'])) {
                            $connector = ' ' . array_keys($value)[0] . ' ';
                            $value = $data[0];
                        }
                    }

                    $likeClauses = [];

                    foreach ($value as $index => $item) {
                        $item = strval($item);

                        if (!preg_match('/((?<!\\\)\[.+(?<!\\\)\]|(?<!\\\)[\*\?\!\%#^_]|%.+|.+%)/', $item)) {
                            $item = '%' . $item . '%';
                        }

                        $likeClauses[] = $column . ($operator === '!~' ? ' NOT' : '') . " LIKE {$mapKey}L{$index}";
                        $map["{$mapKey}L{$index}"] = [$item, PDO::PARAM_STR];
                    }

                    $stack[] = '(' . implode($connector, $likeClauses) . ')';
                } elseif ($operator === '<>' || $operator === '><') {
                    if ($type === 'array') {
                        if ($operator === '><') {
                            $column .= ' NOT';
                        }

                        if ($this->isRaw($value[0]) && $this->isRaw($value[1])) {
                            $stack[] = "({$column} BETWEEN {$this->buildRaw($value[0], $map)} AND {$this->buildRaw($value[1], $map)})";
                        } else {
                            $stack[] = "({$column} BETWEEN {$mapKey}a AND {$mapKey}b)";
                            $dataType = (is_numeric($value[0]) && is_numeric($value[1])) ? PDO::PARAM_INT : PDO::PARAM_STR;

                            $map[$mapKey . 'a'] = [$value[0], $dataType];
                            $map[$mapKey . 'b'] = [$value[1], $dataType];
                        }
                    }
                } elseif ($operator === 'REGEXP') {
                    $stack[] = "{$column} REGEXP {$mapKey}";
                    $map[$mapKey] = [$value, PDO::PARAM_STR];
                } else {
                    throw new InvalidArgumentException("Invalid operator [{$operator}] for column {$column} supplied.");
                }

                continue;
            }

            switch ($type) {

                case 'NULL':
                    $stack[] = $column . ' IS NULL';
                    break;

                case 'array':
                    $placeholders = [];

                    foreach ($value as $index => $item) {
                        $stackKey = $mapKey . $index . '_i';

                        $placeholders[] = $stackKey;
                        $map[$stackKey] = $this->typeMap($item, gettype($item));
                    }

                    $stack[] = $column . ' IN (' . implode(', ', $placeholders) . ')';
                    break;

                case 'object':
                    if ($raw = $this->buildRaw($value, $map)) {
                        $stack[] = "{$column} = {$raw}";
                    }
                    break;

                case 'integer':
                case 'double':
                case 'boolean':
                case 'string':
                    $stack[] = "{$column} = {$mapKey}";
                    $map[$mapKey] = $this->typeMap($value, $type);
                    break;
            }
        }

        return implode($conjunctor . ' ', $stack);
    }

    /**
     * Build the where clause.
     *
     * @param array|null $where
     * @param array $map
     * @return string
     */
    protected function whereClause($where, array &$map): string
    {
        $clause = '';
		

			
        if (is_array($where)) {
            $conditions = array_diff_key($where, array_flip(
                ['GROUP', 'ORDER', 'HAVING', 'LIMIT', 'LIKE', 'MATCH']
            ));

            if (!empty($conditions)) {
                $clause = ' WHERE ' . $this->dataImplode($conditions, $map, ' AND');
            }

            if (isset($where['MATCH']) && $this->type === 'mysql') {
                $match = $where['MATCH'];

                if (is_array($match) && isset($match['columns'], $match['keyword'])) {
                    $mode = '';

                    $options = [
                        'natural' => 'IN NATURAL LANGUAGE MODE',
                        'natural+query' => 'IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION',
                        'boolean' => 'IN BOOLEAN MODE',
                        'query' => 'WITH QUERY EXPANSION'
                    ];

                    if (isset($match['mode'], $options[$match['mode']])) {
                        $mode = ' ' . $options[$match['mode']];
                    }

                    $columns = implode(', ', array_map([$this, 'columnQuote'], $match['columns']));
                    $mapKey = $this->mapKey();
                    $map[$mapKey] = [$match['keyword'], PDO::PARAM_STR];
                    $clause .= ($clause !== '' ? ' AND ' : ' WHERE') . ' MATCH (' . $columns . ') AGAINST (' . $mapKey . $mode . ')';
                }
            }

            if (isset($where['GROUP'])) {
                $group = $where['GROUP'];

                if (is_array($group)) {
                    $stack = [];

                    foreach ($group as $column => $value) {
                        $stack[] = $this->columnQuote($value);
                    }

                    $clause .= ' GROUP BY ' . implode(',', $stack);
                } elseif ($raw = $this->buildRaw($group, $map)) {
                    $clause .= ' GROUP BY ' . $raw;
                } else {
                    $clause .= ' GROUP BY ' . $this->columnQuote($group);
                }
            }

            if (isset($where['HAVING'])) {
                $having = $where['HAVING'];

                if ($raw = $this->buildRaw($having, $map)) {
                    $clause .= ' HAVING ' . $raw;
                } else {
                    $clause .= ' HAVING ' . $this->dataImplode($having, $map, ' AND');
                }
            }

            if (isset($where['ORDER'])) {
                $order = $where['ORDER'];

                if (is_array($order)) {
                    $stack = [];

                    foreach ($order as $column => $value) {
                        if (is_array($value)) {
                            $valueStack = [];

                            foreach ($value as $item) {
                                $valueStack[] = is_int($item) ? $item : $this->quote($item);
                            }

                            $valueString = implode(',', $valueStack);
                            $stack[] = "FIELD({$this->columnQuote($column)}, {$valueString})";
                        } elseif ($value === 'ASC' || $value === 'DESC') {
                            $stack[] = $this->columnQuote($column) . ' ' . $value;
                        } elseif (is_int($column)) {
                            $stack[] = $this->columnQuote($value);
                        }
                    }

                    $clause .= ' ORDER BY ' . implode(',', $stack);
                } elseif ($raw = $this->buildRaw($order, $map)) {
                    $clause .= ' ORDER BY ' . $raw;
                } else {
                    $clause .= ' ORDER BY ' . $this->columnQuote($order);
                }
            }

            if (isset($where['LIMIT'])) {
                $limit = $where['LIMIT'];

                if (in_array($this->type, ['oracle', 'mssql'])) {
                    if ($this->type === 'mssql' && !isset($where['ORDER'])) {
                        $clause .= ' ORDER BY (SELECT 0)';
                    }

                    if (is_numeric($limit)) {
                        $limit = [0, $limit];
                    }

                    if (
                        is_array($limit) &&
                        is_numeric($limit[0]) &&
                        is_numeric($limit[1])
                    ) {
                        $clause .= " OFFSET {$limit[0]} ROWS FETCH NEXT {$limit[1]} ROWS ONLY";
                    }
                } else {
                    if (is_numeric($limit)) {
                        $clause .= ' LIMIT ' . $limit;
                    } elseif (
                        is_array($limit) &&
                        is_numeric($limit[0]) &&
                        is_numeric($limit[1])
                    ) {
                        $clause .= " LIMIT {$limit[1]} OFFSET {$limit[0]}";
                    }
                }
            }
        } elseif ($raw = $this->buildRaw($where, $map)) {
            $clause .= ' ' . $raw;
        }

        return $clause;
    }

    /**
     * Build statement for the select query.
     *
     * @param string $table
     * @param array $map
     * @param array|string $join
     * @param array|string $columns
     * @param array $where
     * @param string $columnFn
     * @return string
     */
    protected function selectContext(
        string $table,
        array &$map,
        $join,
        &$columns = null,
        $where = null,
        $columnFn = null
    ): string {
        preg_match('/(?:(?<schema>[\p{L}_][\p{L}\p{N}@$#\-_]*)\.)?(?<table>[\p{L}_][\p{L}\p{N}@$#\-_]*)\s*\((?<alias>[\p{L}_][\p{L}\p{N}@$#\-_]*)\)/u', $table, $tableMatch);

        if (isset($tableMatch['table'], $tableMatch['alias'])) {
			if(isset($tableMatch['schema'])){
				$schema = $this->schemaQuote($tableMatch['schema']);
			}
            $table = $this->tableQuote($tableMatch['table']);
            $tableAlias = $this->tableQuote($tableMatch['alias']);
            $tableQuery = ($schema?$schema.".":"")."{$table} AS {$tableAlias}";
        } else {
			$schema_table = explode(".",$table);
            $table = $this->tableQuote(end($schema_table));
			if(count($schema_table)==2){
				$table = $this->schemaQuote($schema_table[0]).".".$table;
			}
            $tableQuery = $table;
        }

        $isJoin = $this->isJoin($join);

        if ($isJoin) {
            $tableQuery .= ' ' . $this->buildJoin($tableAlias ?? $table, $join, $map);
        } else {
            if (is_null($columns)) {
                if (
                    !is_null($where) ||
                    (is_array($join) && isset($columnFn))
                ) {
                    $where = $join;
                    $columns = null;
                } else {
                    $where = null;
                    $columns = $join;
                }
            } else {
                $where = $columns;
                $columns = $join;
            }
        }

        if (isset($columnFn)) {
            if ($columnFn === 1) {
                $column = '1';

                if (is_null($where)) {
                    $where = $columns;
                }
            } elseif ($raw = $this->buildRaw($columnFn, $map)) {
                $column = $raw;
            } else {
                if (empty($columns) || $this->isRaw($columns)) {
                    $columns = '*';
                    $where = $join;
                }

                $column = $columnFn . '(' . $this->columnPush($columns, $map, true) . ')';
            }
        } else {
            $column = $this->columnPush($columns, $map, true, $isJoin);
        }

        return 'SELECT ' . $column . ' FROM ' . $tableQuery . $this->whereClause($where, $map);
    }

    /**
     * Determine the array with join syntax.
     *
     * @param mixed $join
     * @return bool
     */
    protected function isJoin($join): bool
    {
        if (!is_array($join)) {
            return false;
        }

        $keys = array_keys($join);

        if (
            isset($keys[0]) &&
            is_string($keys[0]) &&
            strpos($keys[0], '[') === 0
        ) {
            return true;
        }

        return false;
    }

    /**
     * Build the join statement.
     *
     * @param string $table
     * @param array $join
     * @param array $map
     * @return string
     */
    protected function buildJoin(string $table, array $join, array &$map): string
    {
        $tableJoin = [];
        $type = [
            '>' => 'LEFT',
            '<' => 'RIGHT',
            '<>' => 'FULL',
            '><' => 'INNER'
        ];

        foreach ($join as $subtable => $relation) {
            preg_match('/(\[(?<join>\<\>?|\>\<?)\])?(?:(?<schema>[\p{L}_][\p{L}\p{N}@$#\-_]*)\.)?(?<table>[\p{L}_][\p{L}\p{N}@$#\-_]*)\s?(\((?<alias>[\p{L}_][\p{L}\p{N}@$#\-_]*)\))?/u', $subtable, $match);

            if ($match['join'] === '' || $match['table'] === '') {
                continue;
            }

            if (is_string($relation)) {
                $relation = 'USING ("' . $relation . '")';
            } elseif (is_array($relation)) {
                // For ['column1', 'column2']
                if (isset($relation[0])) {
                    $relation = 'USING ("' . implode('", "', $relation) . '")';
                } else {
                    $joins = [];

                    foreach ($relation as $key => $value) {
                        if ($key === 'AND' && is_array($value)) {
                            $joins[] = $this->dataImplode($value, $map, ' AND');
                            continue;
                        }

                        $joins[] = (
                            strpos($key, '.') > 0 ?
                                // For ['tableB.column' => 'column']
                                $this->columnQuote($key) :

                                // For ['column1' => 'column2']
                                $table . '.' . $this->columnQuote($key)
                        ) .
                        ' = ' .
                        ($match['schema']?$this->schemaQuote($match['schema']).".":"").$this->tableQuote($match['alias'] ?? $match['table']) . '.' . $this->columnQuote($value);
                    }

                    $relation = 'ON ' . implode(' AND ', $joins);
                }
            } elseif ($raw = $this->buildRaw($relation, $map)) {
                $relation = $raw;
            }

            $tableName = ($match['schema']?$this->schemaQuote($match['schema']).".":"").$this->tableQuote($match['table']);

            if (isset($match['alias'])) {
                $tableName .= ' AS ' . $this->tableQuote($match['alias']);
            }

            $tableJoin[] = $type[$match['join']] . " JOIN {$tableName} {$relation}";
        }

        return implode(' ', $tableJoin);
    }

    /**
     * Mapping columns for the stack.
     *
     * @param array|string $columns
     * @param array $stack
     * @param bool $root
     * @return array
     */
    protected function columnMap($columns, array &$stack, bool $root): array
    {
        if ($columns === '*') {
            return $stack;
        }

        foreach ($columns as $key => $value) {
            if (is_int($key)) {
                preg_match('/(?:(?<table>[\p{L}_][\p{L}\p{N}@$#\-_]*)\.)?(?<column>[\p{L}_*][\p{L}\p{N}@$#\-_]*)(?:\s*\((?<alias>[\p{L}_][\p{L}\p{N}@$#\-_]*)\))?(?:\s*\[(?<type>(?:String|Bool|Int|Number|Object|JSON))\])?/u', $value, $keyMatch);

                $columnKey = !empty($keyMatch['alias']) ?
                    $keyMatch['alias'] :
                    $keyMatch['column'];
				if($columnKey == "*"){
					$column_list = $this->query("SELECT column_name FROM information_schema.columns WHERE table_name = :table",[":table"=>$keyMatch['table']])->fetchAll(\PDO::FETCH_COLUMN);
					foreach($column_list as $column){
						$stack[$keyMatch["table"].".*"][] = $column;
					}
				} else {
					$stack[$value] = isset($keyMatch['type']) ?
						[$columnKey, $keyMatch['type']] :
						[$columnKey];
				}
            } elseif ($this->isRaw($value)) {
                preg_match('/([\p{L}_][\p{L}\p{N}@$#\-_]*\.)?(?<column>[\p{L}_*][\p{L}\p{N}@$#\-_]*)(\s*\[(?<type>(String|Bool|Int|Number))\])?/u', $key, $keyMatch);
                $columnKey = $keyMatch['column'];

                $stack[$key] = isset($keyMatch['type']) ?
                    [$columnKey, $keyMatch['type']] :
                    [$columnKey];
            } elseif (!is_int($key) && is_array($value)) {
                if ($root && count(array_keys($columns)) === 1) {
                    $stack[$key] = [$key, 'String'];
                }

                $this->columnMap($value, $stack, false);
            }
        }

        return $stack;
    }

    /**
     * Mapping the data from the table.
     *
     * @param array $data
     * @param array $columns
     * @param array $columnMap
     * @param array $stack
     * @param bool $root
     * @param array $result
     * @codeCoverageIgnore
     * @return void
     */
    protected function dataMap(
        array $data,
        array $columns,
        array $columnMap,
        array &$stack,
        bool $root,
        array &$result = null
    ): void {
        if ($root) {
            $columnsKey = array_keys($columns);

            if (count($columnsKey) === 1 && is_array($columns[$columnsKey[0]])) {
                $indexKey = array_keys($columns)[0];
                $dataKey = preg_replace("/^[\p{L}_][\p{L}\p{N}@$#\-_]*\./u", '', $indexKey);
                $currentStack = [];

                foreach ($data as $item) {
                    $this->dataMap($data, $columns[$indexKey], $columnMap, $currentStack, false, $result);
                    $index = $data[$dataKey];

                    if (isset($result)) {
                        $result[$index] = $currentStack;
                    } else {
                        $stack[$index] = $currentStack;
                    }
                }
            } else {
                $currentStack = [];
                $this->dataMap($data, $columns, $columnMap, $currentStack, false, $result);

                if (isset($result)) {
                    $result[] = $currentStack;
                } else {
                    $stack = $currentStack;
                }
            }

            return;
        }

        foreach ($columns as $key => $value) {
            $isRaw = $this->isRaw($value);

            if (is_int($key) || $isRaw) {
                $map_temp = $columnMap[$isRaw ? $key : $value];
				if(is_string($value)&&str_ends_with($value,".*")){
					$column_list = array_map(function($item){return [$item];},$map_temp);
				} else {
					$column_list = [$map_temp];
				}
				foreach($column_list as $map){
					$columnKey = $map[0];
					$item = $data[$columnKey];

					if (isset($map[1])) {
						if ($isRaw && in_array($map[1], ['Object', 'JSON'])) {
							continue;
						}

						if (is_null($item)) {
							$stack[$columnKey] = null;
							continue;
						}

						switch ($map[1]) {

							case 'Number':
								$stack[$columnKey] = (float) $item;
								break;

							case 'Int':
								$stack[$columnKey] = (int) $item;
								break;

							case 'Bool':
								$stack[$columnKey] = (bool) $item;
								break;

							case 'Object':
								$stack[$columnKey] = unserialize($item);
								break;

							case 'JSON':
								$stack[$columnKey] = json_decode($item, true);
								break;

							case 'String':
								$stack[$columnKey] = (string) $item;
								break;
						}
					} else {
						$stack[$columnKey] = $item;
					}
				}
            } else {
                $currentStack = [];
                $this->dataMap($data, $value, $columnMap, $currentStack, false, $result);

                $stack[$key] = $currentStack;
            }
        }
    }

    /**
     * Build and execute returning query.
     *
     * @param string $query
     * @param array $map
     * @param array $data
     * @return \PDOStatement|null
     */
    private function returningQuery($query, &$map, &$data): ?PDOStatement
    {
        $returnColumns = array_map(
            function ($value) {
                return $value[0];
            },
            $data
        );

        $query .= ' RETURNING ' .
                    implode(', ', array_map([$this, 'columnQuote'], $returnColumns)) .
                    ' INTO ' .
                    implode(', ', array_keys($data));

        return $this->exec($query, $map, function ($statement) use (&$data) {
            // @codeCoverageIgnoreStart
            foreach ($data as $key => $return) {
                if (isset($return[3])) {
                    $statement->bindParam($key, $data[$key][1], $return[2], $return[3]);
                } else {
                    $statement->bindParam($key, $data[$key][1], $return[2]);
                }
            }
            // @codeCoverageIgnoreEnd
        });
    }

    /**
     * Create a table.
     *
     * @param string $table
     * @param array $columns Columns definition.
     * @param array $options Additional table options for creating a table.
     * @return \PDOStatement|null
     */
    public function create(string $table, $columns, $options = null): ?PDOStatement
    {
        $stack = [];
        $tableOption = '';
		$schema_table = explode(".",$table);
		$table = $this->tableQuote(end($schema_table));
		if(count($schema_table)==2){
			$table = $this->schemaQuote($schema_table[0]).".".$table;
		}
        $tableName = $table;

        foreach ($columns as $name => $definition) {
            if (is_int($name)) {
                $stack[] = preg_replace('/\<([\p{L}_][\p{L}\p{N}@$#\-_]*)\>/u', '"$1"', $definition);
            } elseif (is_array($definition)) {
                $stack[] = $this->columnQuote($name) . ' ' . implode(' ', $definition);
            } elseif (is_string($definition)) {
                $stack[] = $this->columnQuote($name) . ' ' . $definition;
            }
        }

        if (is_array($options)) {
            $optionStack = [];

            foreach ($options as $key => $value) {
                if (is_string($value) || is_int($value)) {
                    $optionStack[] = "{$key} = {$value}";
                }
            }

            $tableOption = ' ' . implode(', ', $optionStack);
        } elseif (is_string($options)) {
            $tableOption = ' ' . $options;
        }

        $command = 'CREATE TABLE';

        if (in_array($this->type, ['mysql', 'pgsql', 'sqlite'])) {
            $command .= ' IF NOT EXISTS';
        }

        return $this->exec("{$command} {$tableName} (" . implode(', ', $stack) . "){$tableOption}");
    }

    /**
     * Drop a table.
     *
     * @param string $table
     * @return \PDOStatement|null
     */
    public function drop(string $table): ?PDOStatement
    {
		$schema_table = explode(".",$table);
		$table = $this->tableQuote(end($schema_table));
		if(count($schema_table)==2){
			$table = $this->schemaQuote($schema_table[0]).".".$table;
		}
        return $this->exec('DROP TABLE IF EXISTS ' . $table);
    }

    /**
     * Select data from the table.
     *
     * @param string $table
     * @param array $join
     * @param array|string $columns
     * @param array $where
     * @return array|null
     */
    public function select(string $table, $join, $columns = null, $where = null): ?array
    {
        $map = [];
        $result = [];
        $columnMap = [];

        $args = func_get_args();
        $lastArgs = $args[array_key_last($args)];
        $callback = is_callable($lastArgs) ? $lastArgs : null;

        $where = is_callable($where) ? null : $where;
        $columns = is_callable($columns) ? null : $columns;

        $column = $where === null ? $join : $columns;
        $isSingle = (is_string($column) && $column !== '*');

        $statement = $this->exec($this->selectContext($table, $map, $join, $columns, $where), $map);
        $this->columnMap($columns, $columnMap, true);

        if (!$this->statement) {
            return $result;
        }

        // @codeCoverageIgnoreStart
        if ($columns === '*') {
            if (isset($callback)) {
                while ($data = $statement->fetch(PDO::FETCH_ASSOC)) {
                    $callback($data);
                }

                return null;
            }

            return $statement->fetchAll(PDO::FETCH_ASSOC);
        }

        while ($data = $statement->fetch(PDO::FETCH_ASSOC)) {
            $currentStack = [];

            if (isset($callback)) {
                $this->dataMap($data, $columns, $columnMap, $currentStack, true);

                $callback(
                    $isSingle ?
                    $currentStack[$columnMap[$column][0]] :
                    $currentStack
                );
            } else {
                $this->dataMap($data, $columns, $columnMap, $currentStack, true, $result);
            }
        }

        if (isset($callback)) {
            return null;
        }

        if ($isSingle) {
            $singleResult = [];
            $resultKey = $columnMap[$column][0];

            foreach ($result as $item) {
                $singleResult[] = $item[$resultKey];
            }

            return $singleResult;
        }

        return $result;
    }
    // @codeCoverageIgnoreEnd

    /**
     * Insert one or more records into the table.
     *
     * @param string $table
     * @param array $values
     * @param string $primaryKey
     * @return \PDOStatement|null
     */
    public function insert(string $table, array $values, string $primaryKey = null): ?PDOStatement
    {
        $stack = [];
        $columns = [];
        $fields = [];
        $map = [];
        $returnings = [];

        if (!isset($values[0])) {
            $values = [$values];
        }

        foreach ($values as $data) {
            foreach ($data as $key => $value) {
                $columns[] = $key;
            }
        }

        $columns = array_unique($columns);

        foreach ($values as $data) {
            $values = [];

            foreach ($columns as $key) {
                $value = $data[$key];
                $type = gettype($value);

                if ($this->type === 'oracle' && $type === 'resource') {
                    $values[] = 'EMPTY_BLOB()';
                    $returnings[$this->mapKey()] = [$key, $value, PDO::PARAM_LOB];
                    continue;
                }

                if ($raw = $this->buildRaw($data[$key], $map)) {
                    $values[] = $raw;
                    continue;
                }

                $mapKey = $this->mapKey();
                $values[] = $mapKey;

                switch ($type) {

                    case 'array':
                        $map[$mapKey] = [
                            strpos($key, '[JSON]') === strlen($key) - 6 ?
                                json_encode($value) :
                                serialize($value),
                            PDO::PARAM_STR
                        ];
                        break;

                    case 'object':
                        $value = serialize($value);
                        break;

                    case 'NULL':
                    case 'resource':
                    case 'boolean':
                    case 'integer':
                    case 'double':
                    case 'string':
                        $map[$mapKey] = $this->typeMap($value, $type);
                        break;
                }
            }

            $stack[] = '(' . implode(', ', $values) . ')';
        }

        foreach ($columns as $key) {
            $fields[] = $this->columnQuote(preg_replace("/(\s*\[JSON\]$)/i", '', $key));
        }

		$schema_table = explode(".",$table);
		$table = $this->tableQuote(end($schema_table));
		if(count($schema_table)==2){
			$table = $this->schemaQuote($schema_table[0]).".".$table;
		}
        $query = 'INSERT INTO ' . $table . ' (' . implode(', ', $fields) . ') VALUES ' . implode(', ', $stack);

        if (
            $this->type === 'oracle' && (!empty($returnings) || isset($primaryKey))
        ) {
            if ($primaryKey) {
                $returnings[':RETURNID'] = [$primaryKey, '', PDO::PARAM_INT, 8];
            }

            $statement = $this->returningQuery($query, $map, $returnings);

            if ($primaryKey) {
                $this->returnId = $returnings[':RETURNID'][1];
            }

            return $statement;
        }

        return $this->exec($query, $map);
    }

    /**
     * Modify data from the table.
     *
     * @param string $table
     * @param array $data
     * @param array $where
     * @return \PDOStatement|null
     */
    public function update(string $table, $data, $where = null): ?PDOStatement
    {
        $fields = [];
        $map = [];
        $returnings = [];
		

        foreach ($data as $key => $value) {
            $column = $this->columnQuote(preg_replace("/(\s*\[(JSON|\+|\-|\*|\/)\]$)/", '', $key));
            $type = gettype($value);

            if ($this->type === 'oracle' && $type === 'resource') {
                $fields[] = "{$column} = EMPTY_BLOB()";
                $returnings[$this->mapKey()] = [$key, $value, PDO::PARAM_LOB];
                continue;
            }

            if ($raw = $this->buildRaw($value, $map)) {
                $fields[] = "{$column} = {$raw}";
                continue;
            }

            preg_match('/(?<column>[\p{L}_*][\p{L}\p{N}@$#\-_]*)(\[(?<operator>\+|\-|\*|\/)\])?/u', $key, $match);

            if (isset($match['operator'])) {
                if (is_numeric($value)) {
                    $fields[] = "{$column} = {$column} {$match['operator']} {$value}";
                }
            } else {
                $mapKey = $this->mapKey();
                $fields[] = "{$column} = {$mapKey}";

                switch ($type) {

                    case 'array':
                        $map[$mapKey] = [
                            strpos($key, '[JSON]') === strlen($key) - 6 ?
                                json_encode($value) :
                                serialize($value),
                            PDO::PARAM_STR
                        ];
                        break;

                    case 'object':
                        $value = serialize($value);

                        break;
                    case 'NULL':
                    case 'resource':
                    case 'boolean':
                    case 'integer':
                    case 'double':
                    case 'string':
                        $map[$mapKey] = $this->typeMap($value, $type);
                        break;
                }
            }
        }

		$schema_table = explode(".",$table);
		$table = $this->tableQuote(end($schema_table));
		if(count($schema_table)==2){
			$table = $this->schemaQuote($schema_table[0]).".".$table;
		}
        $query = 'UPDATE ' . $table . ' SET ' . implode(', ', $fields) . $this->whereClause($where, $map);

        if ($this->type === 'oracle' && !empty($returnings)) {
            return $this->returningQuery($query, $map, $returnings);
        }

        return $this->exec($query, $map);
    }

    /**
     * Delete data from the table.
     *
     * @param string $table
     * @param array|Raw $where
     * @return \PDOStatement|null
     */
    public function delete(string $table, $where): ?PDOStatement
    {
        $map = [];
		$schema_table = explode(".",$table);
		$table = $this->tableQuote(end($schema_table));
		if(count($schema_table)==2){
			$table = $this->schemaQuote($schema_table[0]).".".$table;
		}

        return $this->exec('DELETE FROM ' . $table . $this->whereClause($where, $map), $map);
    }

    /**
     * Replace old data with a new one.
     *
     * @param string $table
     * @param array $columns
     * @param array $where
     * @return \PDOStatement|null
     */
    public function replace(string $table, array $columns, $where = null): ?PDOStatement
    {
        $map = [];
        $stack = [];

        foreach ($columns as $column => $replacements) {
            if (is_array($replacements)) {
                foreach ($replacements as $old => $new) {
                    $mapKey = $this->mapKey();
                    $columnName = $this->columnQuote($column);
                    $stack[] = "{$columnName} = REPLACE({$columnName}, {$mapKey}a, {$mapKey}b)";

                    $map[$mapKey . 'a'] = [$old, PDO::PARAM_STR];
                    $map[$mapKey . 'b'] = [$new, PDO::PARAM_STR];
                }
            }
        }

        if (empty($stack)) {
            throw new InvalidArgumentException('Invalid columns supplied.');
        }
		$schema_table = explode(".",$table);
		$table = $this->tableQuote(end($schema_table));
		if(count($schema_table)==2){
			$table = $this->schemaQuote($schema_table[0]).".".$table;
		}

        return $this->exec('UPDATE ' . $table . ' SET ' . implode(', ', $stack) . $this->whereClause($where, $map), $map);
    }

    /**
     * Get only one record from the table.
     *
     * @param string $table
     * @param array $join
     * @param array|string $columns
     * @param array $where
     * @return mixed
     */
    public function get(string $table, $join = null, $columns = null, $where = null)
    {
        $map = [];
        $result = [];
        $columnMap = [];
        $currentStack = [];

        if ($where === null) {
            if ($this->isJoin($join)) {
                $where['LIMIT'] = 1;
            } else {
                $columns['LIMIT'] = 1;
            }

            $column = $join;
        } else {
            $column = $columns;
            $where['LIMIT'] = 1;
        }

        $isSingle = (is_string($column) && $column !== '*');
        $query = $this->exec($this->selectContext($table, $map, $join, $columns, $where), $map);

        if (!$this->statement) {
            return false;
        }

        // @codeCoverageIgnoreStart
        $data = $query->fetchAll(PDO::FETCH_ASSOC);

        if (isset($data[0])) {
            if ($column === '*') {
                return $data[0];
            }

            $this->columnMap($columns, $columnMap, true);
            $this->dataMap($data[0], $columns, $columnMap, $currentStack, true, $result);

            if ($isSingle) {
                return $result[0][$columnMap[$column][0]];
            }

            return $result[0];
        }
    }
    // @codeCoverageIgnoreEnd

    /**
     * Determine whether the target data existed from the table.
     *
     * @param string $table
     * @param array $join
     * @param array $where
     * @return bool
     */
    public function has(string $table, $join, $where = null): bool
    {
        $map = [];
        $column = null;

        $query = $this->exec(
            $this->type === 'mssql' ?
                $this->selectContext($table, $map, $join, $column, $where, Medoo::raw('TOP 1 1')) :
                'SELECT EXISTS(' . $this->selectContext($table, $map, $join, $column, $where, 1) . ')',
            $map
        );

        if (!$this->statement) {
            return false;
        }

        // @codeCoverageIgnoreStart
        $result = $query->fetchColumn();

        return $result === '1' || $result === 1 || $result === true;
    }
    // @codeCoverageIgnoreEnd

    /**
     * Randomly fetch data from the table.
     *
     * @param string $table
     * @param array $join
     * @param array|string $columns
     * @param array $where
     * @return array
     */
    public function rand(string $table, $join = null, $columns = null, $where = null): array
    {
        $orderRaw = $this->raw(
            $this->type === 'mysql' ? 'RAND()'
                : ($this->type === 'mssql' ? 'NEWID()'
                : 'RANDOM()')
        );

        if ($where === null) {
            if ($this->isJoin($join)) {
                $where['ORDER'] = $orderRaw;
            } else {
                $columns['ORDER'] = $orderRaw;
            }
        } else {
            $where['ORDER'] = $orderRaw;
        }

        return $this->select($table, $join, $columns, $where);
    }

    /**
     * Build for the aggregate function.
     *
     * @param string $type
     * @param string $table
     * @param array $join
     * @param string $column
     * @param array $where
     * @return string|null
     */
    private function aggregate(string $type, string $table, $join = null, $column = null, $where = null): ?string
    {
        $map = [];

        $query = $this->exec($this->selectContext($table, $map, $join, $column, $where, $type), $map);

        if (!$this->statement) {
            return null;
        }

        // @codeCoverageIgnoreStart
        return (string) $query->fetchColumn();
    }
    // @codeCoverageIgnoreEnd

    /**
     * Count the number of rows from the table.
     *
     * @param string $table
     * @param array $join
     * @param string $column
     * @param array $where
     * @return int|null
     */
    public function count(string $table, $join = null, $column = null, $where = null): ?int
    {
        return (int) $this->aggregate('COUNT', $table, $join, $column, $where);
    }

    /**
     * Calculate the average value of the column.
     *
     * @param string $table
     * @param array $join
     * @param string $column
     * @param array $where
     * @return string|null
     */
    public function avg(string $table, $join, $column = null, $where = null): ?string
    {
        return $this->aggregate('AVG', $table, $join, $column, $where);
    }

    /**
     * Get the maximum value of the column.
     *
     * @param string $table
     * @param array $join
     * @param string $column
     * @param array $where
     * @return string|null
     */
    public function max(string $table, $join, $column = null, $where = null): ?string
    {
        return $this->aggregate('MAX', $table, $join, $column, $where);
    }

    /**
     * Get the minimum value of the column.
     *
     * @param string $table
     * @param array $join
     * @param string $column
     * @param array $where
     * @return string|null
     */
    public function min(string $table, $join, $column = null, $where = null): ?string
    {
        return $this->aggregate('MIN', $table, $join, $column, $where);
    }

    /**
     * Calculate the total value of the column.
     *
     * @param string $table
     * @param array $join
     * @param string $column
     * @param array $where
     * @return string|null
     */
    public function sum(string $table, $join, $column = null, $where = null): ?string
    {
        return $this->aggregate('SUM', $table, $join, $column, $where);
    }

    /**
     * Start a transaction.
     *
     * @param callable $actions
     * @codeCoverageIgnore
     * @return void
     */
    public function action(callable $actions): void
    {
        if (is_callable($actions)) {
            $this->pdo->beginTransaction();

            try {
                $result = $actions($this);

                if ($result === false) {
                    $this->pdo->rollBack();
                } else {
                    $this->pdo->commit();
                }
            } catch (Exception $e) {
                $this->pdo->rollBack();
                throw $e;
            }
        }
    }

    /**
     * Return the ID for the last inserted row.
     *
     * @param string $name
     * @codeCoverageIgnore
     * @return string|null
     */
    public function id(string $name = null): ?string
    {
        $type = $this->type;

        if ($type === 'oracle') {
            return $this->returnId;
        } elseif ($type === 'pgsql') {
            $id = $this->pdo->query('SELECT LASTVAL()')->fetchColumn();

            return (string) $id ?: null;
        }

        return $this->pdo->lastInsertId($name);
    }

    /**
     * Enable debug mode and output readable statement string.
     *
     * @codeCoverageIgnore
     * @return Medoo
     */
    public function debug(): self
    {
        $this->debugMode = true;

        return $this;
    }

    /**
     * Enable debug logging mode.
     *
     * @codeCoverageIgnore
     * @return void
     */
    public function beginDebug(): void
    {
        $this->debugMode = true;
        $this->debugLogging = true;
    }

    /**
     * Disable debug logging and return all readable statements.
     *
     * @codeCoverageIgnore
     * @return void
     */
    public function debugLog(): array
    {
        $this->debugMode = false;
        $this->debugLogging = false;

        return $this->debugLogs;
    }

    /**
     * Return the last performed statement.
     *
     * @codeCoverageIgnore
     * @return string|null
     */
    public function last(): ?string
    {
        if (empty($this->logs)) {
            return null;
        }

        $log = $this->logs[array_key_last($this->logs)];

        return $this->generate($log[0], $log[1]);
    }

    /**
     * Return all executed statements.
     *
     * @codeCoverageIgnore
     * @return string[]
     */
    public function log(): array
    {
        return array_map(
            function ($log) {
                return $this->generate($log[0], $log[1]);
            },
            $this->logs
        );
    }

    /**
     * Get information about the database connection.
     *
     * @codeCoverageIgnore
     * @return array
     */
    public function info(): array
    {
        $output = [
            'server' => 'SERVER_INFO',
            'driver' => 'DRIVER_NAME',
            'client' => 'CLIENT_VERSION',
            'version' => 'SERVER_VERSION',
            'connection' => 'CONNECTION_STATUS'
        ];

        foreach ($output as $key => $value) {
            try {
                $output[$key] = $this->pdo->getAttribute(constant('PDO::ATTR_' . $value));
            } catch (PDOException $e) {
                $output[$key] = $e->getMessage();
            }
        }

        $output['dsn'] = $this->dsn;

        return $output;
    }
    public function patch(string $table, $data, $unique_column_list){
        if(is_string($unique_column_list))$unique_column_list = [$unique_column_list];
        if(!array_is_list($data))$data = [$data];
        $where = [];
        foreach($data as $index=>$row){
            $and = [];
            foreach($unique_column_list as $column){
                $and[$column] = $row[$column];
            }
            $where["AND #".$index] = $and;
        }
        $where = ["OR"=>$where];
        $current_items = $this->select($table,array_merge(array_keys(reset($data)),array("unique"=>$unique_column_list)),$where);
        $update_list = [];
        $insert_list = [];
        $unique_current_items = array_column($current_items,"unique");
        array_walk($current_items,function(&$item){
            unset($item["unique"]);
        });
        foreach($data as $row){
            $unique_row = array_combine($unique_column_list,array_map(function($column) use($row){
                return $row[$column];
            },$unique_column_list));
            if(array_search($unique_row,$unique_current_items) !== false){
                if(array_search($row,$current_items) === false){
                    $update_list[] = $row;
                    $this->update($table,$row,$unique_row);
                }
            } else {
                $insert_list[] = $row;
            }
        }
        if(count($insert_list))$this->insert($table,$insert_list);
    }
    public function sync(string $table, $data, $unique_column_list){
        if(is_string($unique_column_list))$unique_column_list = [$unique_column_list];
        if(!array_is_list($data))$data = [$data];
        $where = [];
        foreach($data as $index=>$row){
            $and = [];
            foreach($unique_column_list as $column){
                $and[$column."[!]"] = $row[$column];
            }
            $where["OR #".$index] = $and;
        }
        $where = ["AND"=>$where];
        $this->delete($table,$where);
        $this->patch($table,$data,$unique_column_list);
    }
}
