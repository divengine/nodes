<?php

declare(strict_types=1);

namespace divengine;

use Closure;
use Exception;

/**
 * [[]] Div PHP Nodes
 *
 * NoSQL Database System for PHP
 *
 * Library for storage and retrieve serialized and relational data/objects
 * only with PHP language.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program as the file LICENSE.txt; if not, please see
 * https://www.gnu.org/licenses/gpl-3.0.txt
 *
 * @package divengine/nodes
 * @author  Rafa Rodriguez [@rafageist] <rafageist@divengine.com>
 * @version 3.0.0
 *
 * @link    https://divengine.org
 * @link    https://github.com/divengine/nodes
 * @link    https://github.com/divengine/nodes/wiki
 */

/* CONSTANTS */
if (!defined("DIV_NODES_ROOT")) {
    define("DIV_NODES_ROOT", "./");
}
if (!defined("DIV_NODES_LOG_FILE")) {
    define("DIV_NODES_LOG_FILE", DIV_NODES_ROOT . "/nodes.log");
}

define("DIV_NODES_FOR_BREAK", "DIV_NODES_FOR_BREAK");
define("DIV_NODES_FOR_CONTINUE_SAVING", "DIV_NODES_FOR_CONTINUE_SAVING");
define("DIV_NODES_FOR_CONTINUE_DISCARDING", "DIV_NODES_FOR_CONTINUE_DISCARDING");
define("DIV_NODES_ROLLBACK_TRANSACTION", "DIV_NODES_ROLLBACK_TRANSACTION");
define("DIV_NODES_FOR_REPLACE_NODE", "DIV_NODES_FOR_REPLACE_NODE");

define("DIV_NODES_TRIGGER_BEFORE_ADD", '__trigger_before_add');
define("DIV_NODES_TRIGGER_AFTER_ADD", '__trigger_after_add');
define("DIV_NODES_TRIGGER_BEFORE_SET", '__trigger_before_set');
define("DIV_NODES_TRIGGER_AFTER_SET", '__trigger_after_set');
define("DIV_NODES_TRIGGER_BEFORE_DEL", '__trigger_before_del');
define("DIV_NODES_TRIGGER_AFTER_DEL", '__trigger_after_del');

define("DIV_NODES_STATS_FOLDER", '.stats');
define("DIV_NODES_INDEX_FOLDER", '.index');
define("DIV_NODES_FIRST_NODE", '.first');
define("DIV_NODES_LAST_NODE", '.last');
define("DIV_NODES_LOCK_NODE", '.lock');
define("DIV_NODES_ORDER_FOLDER", '.order');
define("DIV_NODES_REFERENCES_FOLDER", '.references');
define("DIV_NODES_INVALID_FILENAME_CHARS", ['\\', '/', ':', '*', '?', '"', '<', '>', '|']);
define("DIV_NODES_INDEX_FILE_EXTENSION", 'idx');
define("DIV_NODES_LOCK_FILE_EXTENSION", 'lock');
/**
 * Class divNodes
 */
class nodes
{
    static bool $__log_mode = false;

    static string $__log_file = DIV_NODES_LOG_FILE;

    static string $__version = '3.0.0';

    private ?string $__instance_id = null;

    private array $__triggers = [];

    private array $__indexers = [];

    static array $__trash = [];

    private ?string $schema = null;

    /**
     * Constructor
     *
     * @param string $schema
     */
    public function __construct(string $schema)
    {
        $this->setSchema($schema);
    }

    /**
     * Get current version
     *
     * @return string
     */
    public function getVersion(): string
    {
        return self::$__version;
    }

    /**
     * Return the ID of current instance (generate it if not exists)
     *
     * @return string
     */
    public function getInstanceId(): string
    {
        $this->__instance_id ??= self::generateUUIDv4();
        return $this->__instance_id;
    }

    /**
     * Protect some IDs in schemas
     *
     * @param string|int|float $id
     *
     * @return bool
     */
    protected final function isReservedId(string|int|float $id): bool
    {
        return $id == DIV_NODES_REFERENCES_FOLDER
            || $id == DIV_NODES_INDEX_FOLDER
            || $id == DIV_NODES_STATS_FOLDER
            || $id == DIV_NODES_FIRST_NODE
            || $id == DIV_NODES_LAST_NODE
            || $id == DIV_NODES_LOCK_NODE;
    }

    /**
     * Set the schema of work
     *
     * @param string $schema
     */
    public function setSchema(string $schema): void
    {
        $this->addSchema($schema);
        $this->schema = $schema;
    }

    /**
     * Add schema
     *
     * @param string $schema
     * 
     * @return bool
     */
    public function addSchema(string $schema): bool
    {
        $schemaPath = DIV_NODES_ROOT . "/" . $schema;

        if (!file_exists($schemaPath)) {
            $this->log("Adding schema $schema");
            mkdir($schemaPath, 0777, true);
            return true;
        }

        return false;
    }

    /**
     * Rename a schema
     *
     * @param string $schema
     * @param string $new_name
     *
     * @return bool
     */
    public function renameSchema(string $new_name, string $schema): bool
    {
        $schema ??= $this->schema;

        if (!$this->existsSchema($schema)) {
            return false;
        }

        $restore = $schema === $this->schema;

        rename(DIV_NODES_ROOT . $schema, DIV_NODES_ROOT . $new_name);

        if ($restore) {
            $this->schema = $new_name;
        }

        return true;
    }

    /**
     * Know if schema exists
     *
     * @param string $schema
     *
     * @return bool
     */
    public function existsSchema(?string $schema = null): bool
    {
        $schema ??= $this->schema;

        if (file_exists(DIV_NODES_ROOT . $schema)) {
            if (is_dir(DIV_NODES_ROOT . $schema)) {
                return true;
            }
        }

        self::log("Schema $schema not exists");

        return false;
    }

    /**
     * Switch logs to ON
     * 
     * @return void
     */
    static function logOn(): void
    {
        self::$__log_mode = true;
    }

    /**
     * Switch logs to OFF
     * 
     * @return void
     */
    static function logOff(): void
    {
        self::$__log_mode = false;
    }

    /**
     * Log messages
     *
     * @param string $message
     * @param string $level
     * 
     * @return void
     */
    static function log(string $message, string $level = "INFO"): void
    {
        if (self::$__log_mode) {
            $message = date("Y-m-d h:i:s") . " - [$level] $message \n";
            $f = fopen(self::$__log_file, 'a');

            if (flock($f, LOCK_EX)) {
                fputs($f, $message);
                flock($f, LOCK_UN);
            }

            fclose($f);
        }
    }

    /**
     * Remove a schema
     *
     * @param string  $schema
     *
     * @return bool
     */
    public function delSchema(string $schema): bool
    {
        if (file_exists(DIV_NODES_ROOT . $schema)) {
            if (!is_dir(DIV_NODES_ROOT . $schema)) {
                return false;
            }

            $dir = scandir(DIV_NODES_ROOT . $schema);

            foreach ($dir as $entry) {
                if ($entry != "." && $entry != "..") {
                    if (is_dir(DIV_NODES_ROOT . $schema . "/$entry")) {
                        $this->delSchema($schema . "/$entry");
                    } else {
                        if (!$this->isReservedId($entry)) {
                            $this->delNode($entry, $schema);
                        }
                    }
                }
            }

            // Remove orphan references
            $references = $this->getReferences($schema);

            foreach ($references as $rel) {

                if ($rel['foreign_schema'] == $schema) {
                    $sch = $rel['schema'];
                } else {
                    $sch = $rel['foreign_schema'];
                }

                // If the schema of reference is a sub-schema of this schema
                if ($schema == substr($sch, 0, strlen($schema))) {
                    continue;
                }

                $relations = $this->getReferences($sch);
                $new_references = [];
                foreach ($relations as $relation) {
                    if ($relation['schema'] != $schema && $relation['foreign_schema'] != $schema) {
                        $new_references[] = $relation;
                    }
                }

                file_put_contents(self::clearDoubleSlashes(DIV_NODES_ROOT . "/$sch/" . DIV_NODES_REFERENCES_FOLDER), serialize($new_references));
            }

            @unlink(self::clearDoubleSlashes(DIV_NODES_ROOT . "/$schema/" . DIV_NODES_REFERENCES_FOLDER));
            @unlink(self::clearDoubleSlashes(DIV_NODES_ROOT . "/$schema/" . DIV_NODES_STATS_FOLDER));
            @rmdir(self::clearDoubleSlashes(DIV_NODES_ROOT . "/$schema"));

            return true;
        }

        return false;
    }

    /**
     * Remove one node
     *
     * @param string $id
     * @param ?string $schema
     *
     * @return boolean
     */
    public function delNode(string|int|float $id, ?string $schema = null): bool
    {
        $schema ??= $this->schema;

        if (!$this->existsSchema($schema)) {
            return false;
        }

        $fullNodePath = DIV_NODES_ROOT . "$schema/$id";

        if (!file_exists($fullNodePath)) {
            return false;
        }

        $is_order = false;

        if (
            file_exists(DIV_NODES_ROOT . $schema . '/' . DIV_NODES_FIRST_NODE)
            && file_exists(DIV_NODES_ROOT . $schema . '/' . DIV_NODES_LAST_NODE)
            && !$this->isReservedId($id)
        ) {
            $is_order = true;
        }

        if ($is_order) {
            return $this->waitAndDo(DIV_NODES_ROOT . "$schema", function () use ($schema, $fullNodePath, $id) {
                $raw_data = file_get_contents($fullNodePath);
                $node = @unserialize($raw_data);
                if ($node === false) {
                    $node = $raw_data;
                }

                $r = $this->executeTriggers(DIV_NODES_TRIGGER_BEFORE_DEL, $node, $id, $schema, null, $node);
                if ($r === DIV_NODES_ROLLBACK_TRANSACTION) {
                    return DIV_NODES_ROLLBACK_TRANSACTION;
                }

                // TODO: here or after unlinks? Si es despues hay q tener en cuenta si es raw data

                $r = $this->executeTriggers(DIV_NODES_TRIGGER_AFTER_DEL, $node, $id, $schema, $node, $node);

                if ($r === DIV_NODES_ROLLBACK_TRANSACTION) {
                    return DIV_NODES_ROLLBACK_TRANSACTION;
                }

                // check if is order node (double linked node)
                if (isset($node['previous']) && isset($node['next'])) {
                    if ($node['previous'] === false && $node['next'] === false) // is the only one
                    {
                        $this->setOrderFirst($schema, '');
                        $this->setOrderLast($schema, '');
                    }

                    if ($node['previous'] === false && $node['next'] !== false) // is the first
                    {
                        // update the new first
                        $next = $this->getNode($node['next'], $schema);
                        $next['previous'] = false;
                        $this->putNode($node['next'], $next, $schema);
                        $this->setOrderFirst($schema, $node['next']);
                    }

                    if ($node['next'] === false && $node['previous'] !== false) // is the last
                    {
                        // update the new last
                        $previous = $this->getNode($node['previous'], $schema);
                        $previous['next'] = false;
                        $this->putNode($node['previous'], $previous, $schema);
                        $this->setOrderLast($schema, $node['previous']);
                    }

                    if ($node['next'] !== false && $node['previous'] !== false) // is in the middle
                    {
                        $previous = $this->getNode($node['previous'], $schema);
                        $next = $this->getNode($node['next'], $schema);
                        $previous['next'] = $node['next'];
                        $next['previous'] = $node['previous'];
                        $this->putNode($node['next'], $next, $schema);
                        $this->putNode($node['previous'], $previous, $schema);
                    }
                }

                // Delete the node
                unlink(self::clearDoubleSlashes(DIV_NODES_ROOT . "/$schema/$id"));

                // Delete indexes
                $idx_path = DIV_NODES_ROOT . "/$schema/$id." . DIV_NODES_INDEX_FILE_EXTENSION;
                if (file_exists($idx_path)) {
                    $idx = unserialize(file_get_contents($idx_path));

                    if (isset($idx['indexes'])) {
                        foreach ($idx['indexes'] as $word_schema => $index_id) {
                            $this->delNode($index_id, $word_schema);
                        }
                    }

                    unlink(self::clearDoubleSlashes($idx_path));
                }

                // record stats
                if (!(pathinfo($fullNodePath, PATHINFO_EXTENSION) == DIV_NODES_INDEX_FILE_EXTENSION
                    && file_exists(substr($fullNodePath, 0, strlen($fullNodePath) - 4)))) {
                    $this->changeStats('{count} -= 1', $schema);
                }

                return true;
            });
        } else {
            return $this->waitForNodeAndDo(
                schema: $schema,
                nodeId: $id,
                action: function () use ($schema, $fullNodePath, $id) {
                    $raw_data = file_get_contents($fullNodePath);
                    $node = @unserialize($raw_data);

                    if ($node === false) {
                        $node = $raw_data;
                    }

                    $r = $this->executeTriggers(DIV_NODES_TRIGGER_BEFORE_DEL, $node, $id, $schema, null, $node);
                    if ($r === DIV_NODES_ROLLBACK_TRANSACTION) {
                        return DIV_NODES_ROLLBACK_TRANSACTION;
                    }

                    $restore = [];

                    // Delete cascade
                    $references = $this->getReferences($schema);
                    foreach ($references as $rel) {
                        if ($rel['foreign_schema'] == $schema) {
                            if (!$this->existsSchema($rel['schema'])) {
                                continue;
                            }

                            $ids = $this->getIds($rel['schema']);
                            foreach ($ids as $fid) {
                                $referencedNode = $this->getNode($fid, $rel['schema']);
                                $restore[] = [
                                    "node"   => $referencedNode,
                                    "id"     => $fid,
                                    "schema" => $rel['schema'],
                                ];

                                $delete_node = false;

                                if (is_array($referencedNode)) {
                                    if (isset($referencedNode[$rel['property']])) {
                                        if ($referencedNode[$rel['property']] == $id) {
                                            if ($rel['delete_cascade'] == true) {
                                                $delete_node = true;
                                            } else {
                                                $this->setNode($fid, [
                                                    $rel['property'] => null,
                                                ], $rel['schema']);
                                            }
                                        }
                                    }
                                } elseif (is_object($referencedNode)) {
                                    if (isset($referencedNode->$rel['property'])) {
                                        if ($referencedNode->$rel['property'] == $id) {
                                            if ($rel['delete_cascade'] == true) {
                                                $delete_node = true;
                                            } else {
                                                $this->setNode($fid, [
                                                    $rel['property'] => null,
                                                ], $rel['schema']);
                                            }
                                        }
                                    }
                                }

                                if ($delete_node) {
                                    $r = $this->delNode($fid, $rel['schema']);
                                    if ($r === DIV_NODES_ROLLBACK_TRANSACTION) {
                                        return DIV_NODES_ROLLBACK_TRANSACTION;
                                    }
                                }
                            }
                        }
                    }

                    // Delete the node
                    unlink(self::clearDoubleSlashes(DIV_NODES_ROOT . "/$schema/$id"));

                    $r = $this->executeTriggers(DIV_NODES_TRIGGER_AFTER_DEL, $node, $id, $schema, $node, $node);

                    if ($r === DIV_NODES_ROLLBACK_TRANSACTION) {
                        foreach ($restore as $rest) {
                            if ($this->existsNode($rest['id'], $rest['schema'])) {
                                $this->setNode($rest['id'], $rest['node'], $rest['schema']);
                            } else {
                                $this->addNode($rest['node'], $rest['id'], $rest['schema']);
                            }
                        }

                        return DIV_NODES_ROLLBACK_TRANSACTION;
                    }

                    // Delete indexes
                    $idx_path = DIV_NODES_ROOT . "/$schema/$id." . DIV_NODES_INDEX_FILE_EXTENSION;
                    if (file_exists($idx_path)) {
                        $idx = unserialize(file_get_contents($idx_path));

                        if (isset($idx['indexes'])) {
                            foreach ($idx['indexes'] as $word_schema => $index_id) {
                                $this->delNode($index_id, $word_schema);
                            }
                        }

                        unlink(self::clearDoubleSlashes($idx_path));
                    }

                    // record stats
                    if (!(pathinfo($fullNodePath, PATHINFO_EXTENSION) == DIV_NODES_INDEX_FILE_EXTENSION
                        && file_exists(substr($fullNodePath, 0, strlen($fullNodePath) - 4)))) {
                        $this->changeStats('{count} -= 1', $schema);
                    }

                    return true;
                }
            );
        }
    }

    /**
     * Return a list of schema's references
     *
     * @param string $schema
     *
     * @return array
     */
    public function getReferences(?string $schema = null): array
    {
        $schema ??= $this->schema;

        if (!$this->existsSchema($schema)) {
            return [];
        }

        $path = DIV_NODES_ROOT . $schema . "/.references";
        if (!file_exists($path)) {
            file_put_contents($path, serialize([]));
        }

        $data = file_get_contents($path);

        return unserialize($data);
    }

    /**
     * Return a list of node's id
     *
     * @param string $schema
     *
     * @return array
     */
    public function getIds(?string $schema = null): array
    {
        $schema ??= $this->schema;

        if (!$this->existsSchema($schema)) {
            return [];
        }

        $list = [];
        $dir = scandir(DIV_NODES_ROOT . $schema);

        foreach ($dir as $entry) {
            $full_path = DIV_NODES_ROOT . $schema . "/$entry";
            if (!is_dir($full_path)) {
                if (!$this->isReservedId($entry)) {
                    if (pathinfo($full_path, PATHINFO_EXTENSION) == DIV_NODES_INDEX_FILE_EXTENSION && file_exists(substr($full_path, 0, strlen($full_path) - 4))) {
                        continue;
                    }
                    $list[] = $entry;
                }
            }
        }

        return $list;
    }

    /**
     * Return a node
     *
     * @param mixed   $id
     * @param string  $schema
     * @param mixed   $default
     * @param boolean $keepLocked
     *
     * @return mixed
     */
    public function getNode(string|int|float $id, string $schema = null, mixed $default = null): mixed
    {
        $schema ??= $this->schema;

        // read pure data
        $nodePath = self::clearDoubleSlashes(DIV_NODES_ROOT . "/$schema/$id");

        return $this->waitForNodeAndDo(
            schema: $schema,
            nodeId: $id,
            action: function () use ($nodePath, $default) {
                // ... and load
                if ($default === null) {
                    $data = file_get_contents($nodePath);
                } else {
                    // hide errors if not exists
                    $data = @file_get_contents($nodePath);
                }

                if ($data === false) // the node not exists ...
                {
                    return $default;
                }

                $node = unserialize($data);
                return $node;
            }
        );
    }

    /**
     * Update data of a node
     *
     * @param mixed   $id
     * @param mixed   $data Any data or closure function
     * @param string  $schema
     * @param boolean $cop
     * @param array   $params
     *
     * @return mixed
     */
    public function setNode(string|int|float $id, mixed $data, ?string $schema = null, bool $cop = true, array $params = []): mixed
    {
        $schema ??= $this->schema;

        if (!$this->existsSchema($schema)) {
            return false;
        }

        $setter = null;

        if (is_callable($data)) {
            $setter = $data;
            $data = null;
        }

        return $this->waitForNodeAndDo(
            schema: $schema,
            nodeId: $id,
            action: function () use ($id, $data, $schema, $setter, $cop, $params) {

                if ($this->existsNode($id, $schema)) {

                    $raw_data = file_get_contents(DIV_NODES_ROOT . "$schema/$id");
                    $node = unserialize($raw_data);
                    if ($node == false) {
                        $node = $raw_data;
                    }
                } else {
                    $node = $data;
                }

                $r = $this->executeTriggers(DIV_NODES_TRIGGER_BEFORE_SET, $node, $id, $schema, null, $data);
                if ($r === DIV_NODES_ROLLBACK_TRANSACTION) {
                    return DIV_NODES_ROLLBACK_TRANSACTION;
                }

                $old = $node;
                if ($setter !== null) {
                    $data = $setter($node, $this, $params);
                }

                if ($cop) {
                    $node = $this->cop($node, $data);
                } else {
                    $node = $data;
                }

                file_put_contents(DIV_NODES_ROOT . "$schema/$id", serialize($node));

                $r = $this->executeTriggers(DIV_NODES_TRIGGER_AFTER_SET, $node, $id, $schema, $old, $data);

                if ($r === DIV_NODES_ROLLBACK_TRANSACTION) {
                    file_put_contents(DIV_NODES_ROOT . "$schema/$id", serialize($old));
                }

                return $node;
            }
        );
    }

    /**
     * Replace node
     *
     * @param        $id
     * @param        $data
     * @param string $schema
     * @param array  $params
     *
     * @return mixed
     */
    public function putNode(string|int|float $id, mixed $data, ?string $schema = null, $params = [])
    {
        return $this->setNode($id, $data, $schema, false, $params);
    }

    /**
     * Get default value for type
     * 
     * @param \ReflectionNamedType $type
     * 
     * @return mixed
     */
    private static function getDefaultValueForType(\ReflectionNamedType $type): mixed
    {
        $typeName = $type->getName();

        if ($type->allowsNull()) {
            return null;
        } elseif ($typeName === 'int' || $typeName === 'float') {
            return 0;
        } elseif ($typeName === 'bool') {
            return false;
        } elseif ($typeName === 'string') {
            return '';
        } elseif ($typeName === 'array') {
            return [];
        } else {
            return new $typeName();
        }
    }

    /**
     * Resolve array type from doc
     * 
     * @param string $arrayType
     * 
     * @return string
     */
    public static function resolveArrayType(string $arrayType): string
    {
        if (preg_match('/@var\s+(?:array<([^>\s]+)>\s*|\s*([^>\s]+)\[\]\s*)/', $arrayType, $matches)) {
            return $matches[1] ?: $matches[2];
        }

        return $arrayType;
    }

    /**
     * Compose object/array properties
     *
     * @param mixed   $source
     * @param mixed   $complement
     * @param integer $level
     * @param boolean $strict
     *
     * @param \ReflectionProperty   $propertyType
     * @return mixed
     */
    final public static function cop(mixed &$source, mixed $complement, int $level = 0, bool $strict = false, \ReflectionProperty $propertyType = null): mixed
    {
        $null = null;

        if ($source === null) {
            return $complement;
        }

        if ($complement === null) {
            return $source;
        }

        if (is_scalar($source) && is_scalar($complement)) {
            return $complement;
        }

        if (is_scalar($complement) || is_scalar($source)) {
            return $source;
        }

        if ($level < 100) { // prevent infinite loop
            if (is_object($complement)) {
                $complement = get_object_vars($complement);
            }

            foreach ($complement as $key => $value) {
                if (is_object($source)) {
                    if (property_exists($source, $key)) {
                        $property = new \ReflectionProperty($source, $key);
                        $property->setAccessible(true);

                        if (!$property->isInitialized($source)) {
                            $defaultValue = self::getDefaultValueForType($property->getType());
                            $property->setValue($source, $defaultValue);
                        }

                        $propertyValue = $property->getValue($source);

                        if (is_object($propertyValue) && is_object($value)) {
                            self::cop($propertyValue, $value, $level + 1, $strict, $property);
                        } else {
                            $propertyType = $property->getType();
                            $property->setValue($source, self::cop($propertyValue, $value, $level + 1, $strict, $property));
                        }
                    } else {
                        if (!$strict) {
                            $source->$key = self::cop($null, $value, $level + 1, $strict);
                        }
                    }
                }

                if (is_array($source)) {
                    $updated = false;
                    if ($propertyType !== null) {
                        $docComment = $propertyType->getDocComment();
                        $arrayElementType = self::resolveArrayType($docComment);
                        if (class_exists($arrayElementType)) {
                            $source[$key] = new $arrayElementType();
                            self::cop($source[$key], $value, $level + 1, $strict, $propertyType);
                            $updated = true;
                        }
                    }

                    if (!$updated) {
                        if (array_key_exists($key, $source)) {
                            $source[$key] = self::cop($source[$key], $value, $level + 1, $strict);
                        } else {
                            if (!$strict) {
                                $source[$key] = self::cop($null, $value, $level + 1, $strict);
                            }
                        }
                    }
                }
            }
        }

        return $source;
    }

    /**
     * Know if node exists
     *
     * @param string $id
     * @param string $schema
     *
     * @return boolean
     */
    public function existsNode(string|int|float $id, ?string $schema = null)
    {
        $schema ??= $this->schema;

        $fullPath = DIV_NODES_ROOT . $schema . "/$id";

        if (file_exists($fullPath)) {
            if (!is_dir($fullPath)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Insert a node in schema
     *
     * @param mixed $node
     * @param string|int|float $id
     * @param string $schema
     *
     * @return string|int|float|bool
     */
    public function addNode(mixed $node, string|int|float|null $id = null, ?string $schema = null): string|int|float|bool
    {
        $schema ??= $this->schema;
        $id ??= self::generateUUIDv4();

        if ($this->isReservedId($id) || !$this->isValidId($id)) {
            self::log("Invalid ID '$id' for node");
            return false;
        }

        if ($this->existsNode($id, $schema)) {
            self::log("Node ID '$id' already exists");

            return false;
        }

        $node = $this->executeTriggers(DIV_NODES_TRIGGER_BEFORE_ADD, $node, $id, $schema, null, $node);
        if ($node == false || $node == DIV_NODES_ROLLBACK_TRANSACTION) {
            return DIV_NODES_ROLLBACK_TRANSACTION;
        }

        // save node
        $data = serialize($node);
        file_put_contents(DIV_NODES_ROOT . $schema . "/$id", $data);

        // record the stats
        $full_path = DIV_NODES_ROOT . $schema . "/$id";
        if (!(pathinfo($full_path, PATHINFO_EXTENSION) == DIV_NODES_INDEX_FILE_EXTENSION && file_exists(substr($full_path, 0, strlen($full_path) - 4)))) {
            $this->changeStats('{count} += 1', $schema);
        }

        $r = $this->executeTriggers(DIV_NODES_TRIGGER_AFTER_ADD, $node, $id, $schema, null, $node);

        if ($r === DIV_NODES_ROLLBACK_TRANSACTION) {

            $this->delNode($id, $schema);

            return DIV_NODES_ROLLBACK_TRANSACTION;
        }

        $this->applyIndexers($id, $node, $schema);

        return $id;
    }

    public function applyIndexers(string|int|float $nodeId = null, mixed $node = null, ?string $schema = null): void
    {
        if ($nodeId === null && $node !== null) {
            return;
        }

        $schema ??= $this->schema;

        foreach ($this->__indexers as $name => $indexer) {
            self::log("Applying indexer $name to node $schema/$nodeId");

            $indexer->schema ??= $this->schema;
            
            if ($indexer->schema != $schema) {
                continue;
            }

            self::indexNode(
                nodeId: $nodeId,
                node: $node,
                contentExtractor: $indexer->contentExtractor,
                schema: $schema,
                wholeWords: $indexer->wholeWords ?? false
            );
        }
    }

    /**
     * Add a trigger
     *
     * @param $event
     * @param $callable
     */
    public function addTrigger(string $event, Closure $callable): void
    {
        if (!isset($this->__triggers[$event])) {
            $this->__triggers[$event] = [];
        }

        $this->__triggers[$event][] = $callable;
    }

    /**
     * Execute triggers
     *
     * @param string $event
     * @param mixed  $node
     * @param string $id
     * @param string $schema
     * @param mixed  $old
     * @param mixed  $data
     *
     * @return mixed
     */
    public function executeTriggers(string $event, mixed $node, string|int|float $id, ?string $schema = null, mixed $old = null, mixed $data = null): mixed
    {
        $schema ??= $this->schema;

        if (!isset($this->__triggers[$event])) {
            $this->__triggers[$event] = [];
        }

        foreach ($this->__triggers[$event] as $f) {

            /** @var Closure $f */
            if ($f instanceof Closure) {
                $result = $f($node, $id, $schema, $old, $data);
            } else {
                return DIV_NODES_ROLLBACK_TRANSACTION;
            }

            if ($result == DIV_NODES_ROLLBACK_TRANSACTION) {
                return DIV_NODES_ROLLBACK_TRANSACTION;
            }

            $node = $result;
        }

        return $node;
    }

    /**
     * Return a list of nodes recursively
     *
     * @param string  $schema
     * @param array   $paramsBySchema Apply this params by schema
     * @param array   $paramsDefault  Default params by schema
     * @param integer $offset         From offset
     * @param integer $limit          Limit the result
     * @param bool    $onlyIds        Return only IDs, not the nodes
     *
     * @return array
     */
    public function getRecursiveNodes(string $schema = "/", array $paramsBySchema = [], array $paramsDefault = [], int $offset = 0, int $limit = -1, bool $onlyIds = false): array
    {
        $schemas = [$schema => $schema];
        $schemas = array_merge($schemas, $this->getSchemas($schema));

        $nodes = [];
        foreach ($schemas as $schema) {
            $params = $paramsDefault;
            if (isset($paramsBySchema[$schema])) {
                $params = $paramsBySchema[$schema];
                $params = array_merge($paramsDefault, $params);
            }

            $list = $this->getNodes($params, $schema, true);

            if ($list !== false) {
                $nodes[$schema] = $list;
            }
        }

        // limit result
        $list = [];
        $i = 0;
        $c = 0;
        foreach ($nodes as $schema => $ids) {
            foreach ($ids as $id) {
                if ($i >= $offset) {
                    if ($c < $limit || $limit == -1) {
                        if (!isset($list[$schema])) {
                            $list[$schema] = [];
                        }
                        if (!$onlyIds) {
                            $list[$schema][$id] = $this->getNode($id, $this->schema);
                        } else {
                            $list[$schema][$id] = $id;
                        }
                        $c++;
                    }
                }
            }

            $i++;
        }

        return $list;
    }

    /**
     * Get recursive list of schemas
     *
     * @param $from
     *
     * @return array
     */
    public function getSchemas(string $from): array
    {
        $schemas = [];

        if ($this->existsSchema($from)) {
            $schemas[$from] = $from;

            $stack = [$from => $from];

            while (count($stack) > 0) // avoid recursive calls!!
            {
                $from = array_shift($stack);

                $dir = scandir(DIV_NODES_ROOT . $from);

                foreach ($dir as $entry) {
                    $fullSchema = str_replace("//", "/", "$from/$entry");

                    if ($entry != '.' && $entry != '..' && !is_file(DIV_NODES_ROOT . $fullSchema)) {
                        $stack[$fullSchema] = $fullSchema;
                        $schemas[$fullSchema] = $fullSchema;
                    }
                }
            }
        }

        return $schemas;
    }

    /**
     * Return a list of nodes
     *
     * @param array  $params
     * @param string $schema
     *
     * @return mixed
     */
    public function getNodes(array $params = [], ?string $schema = null, bool $onlyIds = false)
    {
        $schema ??= $this->schema;

        if (!$this->existsSchema($schema)) {
            return false;
        }

        $dp = [
            "offset"    => 0,
            "limit"     => null,
            "fields"    => "*",
            "order"     => null,
            "order_asc" => true,
        ];

        $params = self::cop($dp, $params);

        $data = [
            'params' => $params,
            'ids'    => [],
            'list'   => [],
        ];

        // CASE 1: where, not order, limit
        if (isset($params['where'])) {
            $this->forEachNode(function ($node, $file, $iterator) use (&$data) {

                // if no order...
                if (
                    !isset($data['params']['order'])
                    || (isset($data['params']['order'])
                        && ($data['params']['order'] === false || $data['params']['order'] !== null))
                ) // ..check for offset and limit
                {
                    if (
                        $iterator < $data['params']['offset']
                        || ($data['params']['limit'] <= 0
                            && $data['params']['limit'] !== null)
                    ) {
                        return DIV_NODES_FOR_CONTINUE_DISCARDING;
                    }
                }

                $vars = [];
                if (is_object($node)) {
                    $vars = get_object_vars($node);
                } elseif (is_array($node)) {
                    $vars = $node;
                } elseif (is_scalar($node)) {
                    $vars = ['value' => $node];
                }

                $w = $data['params']['where'];

                foreach ($vars as $key => $value) {
                    $w = str_replace('{' . $key . '}', '$vars["' . $key . '"]', $w);
                }

                $w = str_replace('{id}', '$id', $w);

                $r = false;
                $st = '$r = ' . $w . ';';
                eval($st);

                if ($r === true) {
                    $data['ids'][] = $file;
                    $data['list'][$file] = $node;
                }

                $data['params']['limit']--;
            }, $schema);
        } // CASE 2: not where, not order, limit
        elseif (!isset($params['order']) || (isset($params['order']) && ($params['order'] === false || $params['order'] === null))) {
            $this->forEachNode(function ($node, $file, $iterator) use (&$data) {
                // get nodes without order
                if ($iterator >= $data['params']['offset'] && ($data['params']['limit'] > 0 || $data['params']['limit'] === null)) {
                    $data['ids'][] = $file;
                    $data['list'][$file] = $node;
                }

                if ($data['params']['limit'] !== null) {
                    $data['params']['limit']--;
                }
            }, $schema);
        } // CASE 3: not where, order + limit (get all for sort after)
        else {
            $this->forEachNode(function ($node, $file, $iterator) use (&$data) {
                $data['ids'][] = $file;
                $data['list'][$file] = $node;
            }, $schema);
        }

        $newIds = $data['ids'];

        // sort results
        if (isset($params['order'])) {
            $order = $params['order'];

            if ($order !== false && $order !== null) {
                $sorted = [];
                foreach ($newIds as $id) {
                    $node = $data['list'][$id];
                    $sorted[$id] = $node;
                    if (is_object($node) && isset($node->$order)) {
                        $sorted[$id] = $node->$order;
                    }
                    if (is_array($node) && isset($node[$order])) {
                        $sorted[$id] = $node[$order];
                    }
                }

                if (asort($sorted)) {
                    if ($params['order_asc'] === false) {
                        $sorted = array_reverse($sorted);
                    }
                    $newIds = array_keys($sorted);
                }
            }

            // limit result
            $list = [];
            $i = 0;
            $c = 0;
            foreach ($newIds as $id) {
                if ($i >= $params['offset']) {
                    if ($c < $params['limit'] || $params['limit'] === null) {
                        $list[] = $id;
                        $c++;
                    }
                }
                $i++;
            }

            $newIds = $list;
        }

        if ($onlyIds) {
            return $newIds;
        }


        $list = [];
        foreach ($newIds as $id) {
            $list[$id] = $data['list'][$id];
        }

        return $list;
    }

    /**
     * Return the count of nodes
     *
     * @param array  $params
     * @param string $schema
     *
     * @return int|bool
     */
    public function getCount(array $params = [], ?string $schema = null): int|bool
    {
        $schema ??= $this->schema;

        if (!$this->existsSchema($schema)) {
            return false;
        }

        $dp = [
            "where" => "true",
        ];

        $params = self::cop($dp, $params);

        $ids = $this->getIds($schema);

        $c = 0;
        foreach ($ids as $id) {
            $node = $this->getNode($id, $schema);

            if (is_object($node)) {
                $vars = get_object_vars($node);
            } elseif (is_array($node)) {
                $vars = $node;
            } elseif (is_scalar($node)) {
                $vars = [
                    'value' => $node,
                ];
            }

            $w = $params['where'];
            foreach ($vars as $key => $value) {
                $w = str_replace('{' . $key . '}', '$vars["' . $key . '"]', $w);
            }
            $w = str_replace('{id}', $id, $w);

            $r = false;
            eval('$r = ' . $w . ';');
            if ($r === true) {
                $c++;
            }
        }

        return $c;
    }

    /**
     * Remove some nodes
     *
     * @param array  $params
     * @param string $schema
     *
     * @return boolean
     */
    public function delNodes(array $params = [], ?string $schema = null): bool
    {
        $schema ??= $this->schema;

        if (!$this->existsSchema($schema)) {
            return false;
        }

        $dp = [
            "where"  => "true",
            "offset" => 0,
            "limit"  => -1,
        ];
        $params = self::cop($dp, $params);

        if ($params['where'] != "true") {
            $nodes = $this->getNodes($params, $schema);
            foreach ($nodes as $id => $node) {
                $this->delNode($id, $schema);
            }
        } else {
            $nodes = $this->getIds($schema);
            foreach ($nodes as $id) {
                $this->delNode($id, $schema);
            }
        }

        return true;
    }

    /**
     * Set id of Node
     *
     * @param string $oldId
     * @param string $newId
     * @param string $schema
     *
     * @return boolean
     */
    public function setNodeID(string|int|float $oldId, string|int|float $newId, ?string $schema = null): bool
    {
        return $this->renameNode($oldId, $newId, $schema);
    }

    /**
     * Add new reference for schema
     *
     * @param array $params
     *
     * @return boolean
     */
    public function addReference(array $params = []): bool
    {
        $dp = [
            "schema"         => $this->schema,
            "foreign_schema" => $this->schema,
            "update_cascade" => true,
            "delete_cascade" => true,
        ];

        $params = self::cop($dp, $params);

        if (!isset($params['property'])) {
            return false;
        }

        $schema = $params['schema'];
        $foreign_schema = $params['foreign_schema'];

        if (!$this->existsSchema($schema)) {
            return false;
        }
        if (!$this->existsSchema($foreign_schema)) {
            return false;
        }

        $references = $this->getReferences($schema);
        $foreignReferences = $this->getReferences($foreign_schema);

        foreach ($references as $rel) {
            if (serialize($rel) == serialize($params)) {
                return true;
            }
        }

        $references[] = $params;
        $foreignReferences[] = $params;

        file_put_contents(DIV_NODES_ROOT . $schema . '/' . DIV_NODES_REFERENCES_FOLDER, serialize($references));
        file_put_contents(DIV_NODES_ROOT . $foreign_schema . '/' . DIV_NODES_REFERENCES_FOLDER, serialize($foreignReferences));

        return true;
    }

    /**
     * Delete a reference
     *
     * @param array $params
     *
     * @return boolean
     */
    public function delReference(array $params = []): bool
    {
        $dp = [
            "schema"         => $this->schema,
            "foreign_schema" => $this->schema,
            "update_cascade" => true,
            "delete_cascade" => true,
        ];

        $params = self::cop($dp, $params);

        if (!isset($params['property'])) {
            return false;
        }

        $schema = $params['schema'];
        $foreign_schema = $params['foreign_schema'];

        $references = $this->getReferences($schema);
        $new_references = [];
        foreach ($references as $rel) {
            if ($rel['schema'] == $params['schema'] && $rel['foreign_schema'] == $params['foreign_schema'] && $rel['property'] == $params['property']) {
                continue;
            }
            $new_references[] = $rel;
        }

        file_put_contents(DIV_NODES_ROOT . $schema . "/.references", serialize($new_references));

        $references = $this->getReferences($foreign_schema);
        $new_references = [];
        foreach ($references as $rel) {
            if ($rel['schema'] == $params['schema'] && $rel['foreign_schema'] == $params['foreign_schema'] && $rel['property'] == $params['property']) {
                continue;
            }

            $new_references[] = $rel;
        }

        file_put_contents(DIV_NODES_ROOT . $foreign_schema . "/.references", serialize($new_references));

        return true;
    }

    /**
     * Foreach
     *
     * @param mixed  $closure
     * @param string $schema
     * @param array  $otherData
     */
    public function forEachNode(Closure $closure, ?string $schema = null): void
    {
        $iterator = 0;
        $schema ??= $this->schema;

        if ($dir = opendir(DIV_NODES_ROOT . $schema)) {
            while (($file = readdir($dir)) !== false) {
                $full_path = DIV_NODES_ROOT . $schema . "/" . $file;

                if (pathinfo($full_path, PATHINFO_EXTENSION) == DIV_NODES_INDEX_FILE_EXTENSION) {
                    continue;
                }

                if (pathinfo($full_path, PATHINFO_EXTENSION) == DIV_NODES_LOCK_FILE_EXTENSION) {
                    continue;
                }

                if (!$this->isReservedId($file) && $file != "." && $file != ".." && !is_dir($full_path)) {
                    $node = $this->getNode($file, $schema);
                    $md5 = md5(serialize($node));

                    $result = $closure($node, $file, $iterator);

                    if ($result == DIV_NODES_FOR_BREAK) {
                        break;
                    }
                    if ($result == DIV_NODES_FOR_CONTINUE_DISCARDING) {
                        continue;
                    }

                    // default: DIV_NODES_FOR_CONTINUE_SAVING)

                    $new_md5 = md5(serialize($node));
                    if ($md5 != $new_md5) {
                        if ($result == DIV_NODES_FOR_REPLACE_NODE) {
                            $this->putNode($file, $node, $schema);
                        } else {
                            $this->setNode($file, $node, $schema);
                        }
                    }

                    $iterator++;
                }
            }
            closedir($dir);
        }
    }

    /**
     * Get words from content
     *
     * @param mixed  $content
     * @param string $chars
     *
     * @return array
     */
    public function getWords($content, $chars = ' abcdefghijklmnopqrstuvwxyz1234567890')
    {
        $content = "$content";

        $l = strlen($content);

        $new_content = '';
        for ($i = 0; $i < $l; $i++) {
            if (stripos($chars, $content[$i]) !== false) {
                $new_content .= $content[$i];
            } else {
                $new_content .= ' ';
            }
        }

        $new_content = trim(strtolower($new_content));

        while (strpos($new_content, '  ') !== false) {
            $new_content = str_replace('  ', ' ', $new_content);
        }

        $words = explode(' ', $new_content);

        $new_words = [];
        foreach ($words as $word) {
            $new_words[$word] = $word;
        }

        return $new_words;
    }

    /**
     * Add index of node
     *
     * @param array   $words
     * @param string  $nodeId
     * @param string  $schema
     * @param ?string    $indexSchema
     * @param boolean $wholeWords
     */
    public function addIndex($words, $nodeId, $schema = null, $indexSchema = null, $wholeWords = false)
    {
        $schema ??= $this->schema;
        $indexSchema ??= $schema . '/' . DIV_NODES_INDEX_FOLDER;

        $this->addSchema($indexSchema);

        $pathToNode = "$schema/$nodeId";
        $id = md5($pathToNode);

        foreach ($words as $word) {
            $l = strlen($word);
            if ($wholeWords) {
                $wordSchema = $word;
            } else {
                $wordSchema = '';
                for ($i = 0; $i < $l; $i++) {
                    $wordSchema .= $word[$i] . '/';
                }
            }

            $wordSchema = "$indexSchema/$wordSchema";

            $this->addSchema($wordSchema);

            $node = $this->getNode($id, $wordSchema, [
                "schema"      => $schema,
                "id"          => $nodeId,
                "path"        => $pathToNode,
                "last_update" => date("Y-m-d h:i:s"),
            ]);

            if ($this->existsNode($id, $wordSchema)) {
                $this->setNode($id, $node, $wordSchema);
            } else {
                $this->addNode($node, $id, $wordSchema);
            }

            $this->addInverseIndex($nodeId, $schema, $id, $wordSchema);
        }
    }

    private function defaultContentExtractor()
    {
        return function ($node, $nodeId) {
            $content = '';

            if (is_object($node)) {
                if (method_exists($node, '__toContent')) {
                    $content = $node->__toContent();
                } elseif (method_exists($node, '__toString')) {
                    $content = "$node";
                }
            } elseif (is_scalar($node)) {
                $content = "$node";
            }

            return $content;
        };
    }
    /**
     * Create index of schema
     *
     * @param Closure $contentExtractor
     * @param string  $schema
     * @param string  $indexSchema
     * @param boolean $wholeWords
     * @param boolean $clearFirst
     * 
     * @return void
     */
    public function createIndex(Closure $contentExtractor = null, ?string $schema = null, ?string $indexSchema = null, bool $wholeWords = false, bool $clearFirst = false)
    {
        if ($clearFirst) {
            $schema ??= $this->schema;
            $indexSchema ??= $schema . '/' . DIV_NODES_INDEX_FOLDER;
            $this->delSchema($indexSchema);
        }

        $this->forEachNode(function ($node, $nodeId, $iterator) use ($schema, $contentExtractor, $wholeWords, $indexSchema) {
            $this->indexNode($nodeId, $node, $contentExtractor, $schema, $indexSchema, $wholeWords);
        }, $schema);
    }

    private function indexNode(string|int|float|null $nodeId = null, mixed $node = null, Closure $contentExtractor = null, ?string $schema = null, ?string $indexSchema = null, bool $wholeWords = false): bool
    {
        if ($node === null && $nodeId === null) {
            return false;
        }

        $contentExtractor ??= $this->defaultContentExtractor();
        $schema ??= $this->schema;
        $indexSchema ??= $schema . '/' . DIV_NODES_INDEX_FOLDER;
        $node ??= $this->getNode($nodeId, $schema);
        $words = [];
        $extract_words = true;

        // extract words from built-in node method
        if (is_object($node)) {
            if (method_exists($node, '__toWords')) {
                $extract_words = false;
                $words = $node->__toWords();
            }
        }

        // extract words
        if ($extract_words && count($words) == 0) {
            // get content
            $content = $contentExtractor($node, $nodeId);
            $words = $this->getWords($content);
        }

        $words = $this->getWords($content);

        $this->addIndex($words, $nodeId, $schema, $indexSchema, $wholeWords);

        return true;
    }

    /**
     * Define an index
     * 
     * @param string  $name
     * @param Closure $contentExtractor
     * @param string  $schema
     * @param string  $indexSchema
     * @param boolean $wholeWords
     * 
     * @return void
     */
    public function addIndexer(string $name, Closure $contentExtractor = null, ?string $schema = null, ?string $indexSchema = null, bool $wholeWords = false)
    {
        $this->__indexers[$name] = (object) [
            'contentExtractor' => $contentExtractor,
            'schema'           => $schema,
            'indexSchema'      => $indexSchema,
            'wholeWords'       => $wholeWords
        ];
    }

    /**
     * Full text search
     *
     * @param string $phrase
     * @param ?string $indexSchema
     * @param int    $offset
     * @param int    $limit
     *
     * @return array
     */
    public function search(string $phrase, ?string $indexSchema = null, int $offset = 0, int $limit = -1): array
    {
        $indexSchema ??= $this->schema . '/' . DIV_NODES_INDEX_FOLDER;
        $results = [];
        $words = $this->getWords($phrase);
        $words[$phrase] = $phrase;

        foreach ($words as $word) {
            // build schema from word
            $l = strlen($word);

            for ($wholeWords = 0; $wholeWords < 2; $wholeWords++) {
                $schema = '';
                if ($wholeWords == 0) {
                    for ($i = 0; $i < $l; $i++) {
                        $schema .= $word[$i] . "/";
                    }
                } else {
                    $schema = $word;
                } // whole word

                $schema = "$indexSchema/$schema";

                if ($this->existsSchema($schema)) {
                    // get indexes
                    $schemas = $this->getRecursiveNodes($schema, [], [], $offset, $limit);

                    // calculate score
                    foreach ($schemas as $sch => $nodes) {
                        foreach ($nodes as $id => $node) {
                            //$id = md5($node['path']);

                            if (!isset($results[$id])) {
                                $node['score'] = 0;
                                $results[$id] = $node;
                            }

                            $results[$id]['score']++;
                        }
                    }
                }
            }
        }

        // sort results
        uasort($results, function ($a, $b) {
            if ($a['score'] == $b['score']) {
                return 0;
            }

            return $a['score'] > $b['score'] ? -1 : 1;
        });

        return $results;
    }

    /**
     * Default stats structure
     *
     * @return array
     */
    private function defaultStats(): array
    {
        return ['count' => 0];
    }

    /**
     * Return the stats of schema
     *
     * @param ?string $schema
     *
     * @return array|mixed
     */
    public function getStats(?string $schema = null): array|null
    {
        $stats = null;

        if (!$this->existsSchema($schema)) {
            return null;
        }

        if ($this->existsNode(DIV_NODES_STATS_FOLDER, $schema)) {
            $stats = $this->getNode(DIV_NODES_STATS_FOLDER, $schema, null);
        }

        $stats ??= $this->reStats($schema);

        return $stats;
    }

    /**
     * Secure change of stats
     *
     * @param      $change
     * @param null $schema
     *
     * @return array|mixed
     */
    public function changeStats($change, ?string $schema = null)
    {
        $schema ??= $this->schema;

        return $this->setNode(DIV_NODES_STATS_FOLDER, function ($stats, nodes $db, $params = []) {

            $change = $params['change'];

            if (empty($stats)) {
                $stats = $db->defaultStats();
            }

            if (is_string($change)) {
                // change stats
                $expression = $change;
                foreach ($stats as $key => $value) {
                    $expression = str_replace('{' . $key . '}', '$stats["' . $key . '"]', $expression);
                }
                @eval($expression . ";");
            } elseif (is_callable($change)) {
                $change($stats);
            }

            return $stats;
        }, $schema, true, [
            "change" => $change,
        ]);
    }

    /**
     * Re-write stats of schema
     *
     * @param ?string $schema
     *
     * @return array
     */
    public function reStats(?string $schema = null): array
    {
        $schema ??= $this->schema;

        $stats = $this->defaultStats();

        // 'count' stat
        $this->forEachNode(function ($node, $file, $iterator) use (&$stats) {
            $stats['count']++;
            return DIV_NODES_FOR_CONTINUE_DISCARDING;
        }, $schema);

        // save stat
        $this->delNode(DIV_NODES_STATS_FOLDER, $schema);

        // no use addNode!
        file_put_contents(DIV_NODES_ROOT . $schema . '/' . DIV_NODES_STATS_FOLDER, serialize($stats));

        return $stats;
    }

    /**
     * Rename node
     *
     * @param string $oldId
     * @param string $newId
     * @param string $schema
     *
     * @return boolean
     */
    public function renameNode(string|int|float $oldId, string|int|float $newId, ?string $schema = null): bool
    {
        $schema ??= $this->schema;
        if ($this->existsNode($newId, $schema)) {
            return false;
        }
        if (!$this->existsNode($oldId, $schema)) {
            return false;
        }

        return $this->waitForNodeAndDo(
            schema: $schema,
            nodeId: $oldId,
            action: function () use ($oldId, $newId, $schema) {
                // update references
                $restore = [];
                $references = $this->getReferences($schema);

                foreach ($references as $rel) {
                    if ($rel['foreign_schema'] == $schema) {
                        if (!$this->existsSchema($rel['schema'])) {
                            continue;
                        }

                        $ids = $this->getIds($rel['schema']);

                        foreach ($ids as $fid) {
                            $node = $this->getNode($fid, $rel['schema']);

                            $restore[] = [
                                "node"   => $node,
                                "id"     => $fid,
                                "schema" => $rel['schema'],
                            ];

                            if (is_array($node)) {
                                if (isset($node[$rel['property']])) {
                                    if ($node[$rel['property']] == $oldId) {
                                        $this->setNode($fid, [
                                            $rel['property'] => $newId,
                                        ], $rel['schema']);
                                    }
                                }
                            } elseif (is_object($node)) {
                                if (isset($node->$rel['property'])) {
                                    if ($node->$rel['property'] == $oldId) {
                                        $this->setNode($fid, [
                                            $rel['property'] => $newId,
                                        ], $rel['schema']);
                                    }
                                }
                            }
                        }
                    }
                }

                // update indexes
                if (file_exists(DIV_NODES_ROOT . $schema . "/$oldId." . DIV_NODES_INDEX_FILE_EXTENSION)) {
                    $idx = $this->getNode("$oldId." . DIV_NODES_INDEX_FILE_EXTENSION, $schema);

                    foreach ($idx['indexes'] as $wordSchema => $index) {
                        // update index
                        $pathToNode = "$schema/$newId";
                        $nodeIndex = $this->getNode($index, $wordSchema);
                        $nodeIndex['id'] = $newId;
                        $nodeIndex['last_update'] = date("Y-m-d h:i:s");
                        $nodeIndex['path'] = $pathToNode;

                        $this->setNode($index, $nodeIndex, $wordSchema);

                        // rename index (recursive call)

                        $newIndex = md5($pathToNode);
                        $this->renameNode($index, $newIndex, $wordSchema);

                        // update inverse indexes
                        $idx['indexes'][$wordSchema] = $newIndex;
                    }

                    // update inverse indexes
                    $idx["last_update"] = date("Y-m-d h:i:s");
                    $this->putNode("$oldId." . DIV_NODES_INDEX_FILE_EXTENSION, $idx, $schema);

                    // real rename of idx file
                    rename(DIV_NODES_ROOT . $schema . "/$oldId." . DIV_NODES_INDEX_FILE_EXTENSION, DIV_NODES_ROOT . $schema . "/$newId." . DIV_NODES_INDEX_FILE_EXTENSION);
                }

                // real rename of node file
                rename(DIV_NODES_ROOT . $schema . "/$oldId", DIV_NODES_ROOT . $schema . "/$newId");

                return true;
            }
        );
    }

    /**
     * Return the first node in order's schema
     *
     * @param string $schemaTag
     *
     * @return mixed
     */
    public function getOrderFirst(string $schemaTag): mixed
    {
        $first = $this->getNode(DIV_NODES_FIRST_NODE, $schemaTag, false);
        if ($first !== false) {
            if (empty($first['id'])) {
                $first = false;
            }
        }

        return $first;
    }

    /**
     * Set the first node in order's schema
     *
     * @param string $schemaTag
     * @param string|int|float $orderId
     *
     * @return mixed
     */
    private function setOrderFirst(string $schemaTag, string|int|float $orderId)
    {
        return $this->putNode(DIV_NODES_FIRST_NODE, [
            'id'          => $orderId,
            'last_update' => date("Y-m-d h:i:s"),
        ], $schemaTag);
    }

    /**
     * Set the last node in order's schema
     *
     * @param string $schemaTag
     * @param string $orderId
     *
     * @return mixed
     */
    private function setOrderLast(string $schemaTag, string|int|float $orderId)
    {
        return $this->putNode(DIV_NODES_LAST_NODE, [
            'id'          => $orderId,
            'last_update' => date("Y-m-d h:i:s"),
        ], $schemaTag);
    }

    /**
     * Return the last node in order's schema
     *
     * @param string $schemaTag
     *
     * @return mixed
     */
    public function getOrderLast(string $schemaTag): mixed
    {
        $last = $this->getNode(DIV_NODES_LAST_NODE, $schemaTag, false);
        if ($last !== false) {
            if (empty($last['id'])) {
                $last = false;
            }
        }

        return $last;
    }

    /**
     * Add order
     *
     * @param mixed  $value
     * @param string|int|float $nodeId
     * @param string $tag
     * @param ?string $schema
     * @param ?string $schemaOrder
     *
     * @return bool
     * 
     * @throws Exception
     */
    public function addOrder(mixed $value, string|int|float $nodeId, string $tag = 'default', ?string $schema = null, ?string $schemaOrder = null): bool
    {
        $schema ??= $this->schema;
        $schemaOrder ??= $schema . '/' . DIV_NODES_ORDER_FOLDER;
        $schemaTag = "$schemaOrder/$tag";

        $this->addSchema($schemaTag);

        // wait for unlocked list
        return $this->waitAndDo(DIV_NODES_ROOT . "$schemaTag", function () use ($schemaTag, $schema, $nodeId, $value) {
            $newNode = false;
            $orderId = md5("$schema/$nodeId");
            $first = $this->getOrderFirst($schemaTag);
            $last = $this->getOrderLast($schemaTag);

            // check if no nodes
            if ($first === false) {
                // insert the first
                $this->setOrderFirst($schemaTag, $orderId);
                $this->setOrderLast($schemaTag, $orderId);

                $newNode = [
                    "schema"      => $schema,
                    "id"          => $nodeId,
                    "next"        => false,
                    "previous"    => false,
                    "value"       => $value,
                    'last_update' => date("Y-m-d h:i:s"),
                ];
            } else {

                $firstOrder = $this->getNode($first['id'], $schemaTag);
                $lastOrder = $this->getNode($last['id'], $schemaTag);
                $current = $first['id'];
                $currentOrder = $firstOrder;

                do {
                    if ($currentOrder['value'] > $value) {
                        if ($currentOrder['previous'] === false) // insert on top
                        {
                            $newNode = [
                                "schema"      => $schema,
                                "id"          => $nodeId,
                                "next"        => $current,
                                "previous"    => false,
                                "value"       => $value,
                                'last_update' => date("Y-m-d h:i:s"),
                            ];

                            $currentOrder['previous'] = $orderId;
                            $this->putNode($current, $currentOrder, $schemaTag);
                            $this->setOrderFirst($schemaTag, $orderId);
                            break;
                        }

                        // insert before
                        $newNode = [
                            "schema"      => $schema,
                            "id"          => $nodeId,
                            "next"        => $current,
                            "previous"    => $currentOrder['previous'],
                            "value"       => $value,
                            'last_update' => date("Y-m-d h:i:s"),
                        ];

                        $previous = $currentOrder['previous'];
                        $currentOrder['previous'] = $orderId;
                        $previousNode = $this->getNode($previous, $schemaTag);
                        $previousNode['next'] = $orderId;

                        $this->putNode($current, $currentOrder, $schemaTag);
                        $this->putNode($previous, $previousNode, $schemaTag);
                        break;
                    }

                    if ($currentOrder['next'] === false) {
                        break;
                    }

                    $current = $currentOrder['next'];
                    $currentOrder = $this->getNode($current, $schemaTag);
                } while ($currentOrder['next'] !== false);

                // insert on bottom
                if ($newNode === false) {
                    $lastOrder['next'] = $orderId;
                    $this->putNode($last['id'], $lastOrder, $schemaTag);

                    $newNode = [
                        "schema"      => $schema,
                        "id"          => $nodeId,
                        "next"        => false,
                        "previous"    => $last['id'],
                        "value"       => $value,
                        'last_update' => date("Y-m-d h:i:s"),
                    ];

                    $this->setOrderLast($schemaTag, $orderId);
                }
            }

            if ($newNode !== false) {
                $this->addNode($newNode, $orderId, $schemaTag);
                $this->addInverseIndex($nodeId, $schema, $orderId, $schemaTag);

                return true;
            }

            return false;
        });
    }

    /**
     * For each order
     *
     * @param mixed   $closure
     * @param string  $tag
     * @param integer $offset
     * @param integer $limit
     * @param bool    $fromFirst
     * @param array   $otherData
     * @param string  $schema
     * @param string  $schemaOrder
     */
    public function foreachOrder(array|Closure $closure, string $tag = 'default', int $offset = 0, int $limit = -1, bool $fromFirst = true, ?string $schema = null, ?string $schemaOrder = null): void
    {
        if (is_array($closure)) {
            $tag = $closure['tag'] ?? $tag;
            $offset = $closure['offset'] ?? $offset;
            $limit = $closure['limit'] ?? $limit;
            $fromFirst = $closure['fromFirst'] ?? $fromFirst;
            $schema = $closure['schema'] ?? $schema;
            $schemaOrder = $closure['schemaOrder'] ?? $schemaOrder;
            $closure = $closure['closure'] ?? function () { 
            };
        }

        $schema ??= $this->schema;
        $schemaOrder ??= $schema . '/' . DIV_NODES_ORDER_FOLDER;
        $schemaTag = "$schemaOrder/$tag";

        $this->addSchema($schemaTag);

        $first = $this->getOrderFirst($schemaTag);
        $last = $this->getOrderLast($schemaTag);

        if ($first !== false) {
            $firstOrder = $this->getNode($first['id'], $schemaTag);
            $lastOrder = $this->getNode($last['id'], $schemaTag);
            $currentNode = $fromFirst ? $firstOrder : $lastOrder;
            $iterator = -1;

            do {

                $iterator++;

                if ($iterator < $offset) {
                    continue;
                }

                $result = $closure($currentNode, $iterator);

                if ($result == DIV_NODES_FOR_BREAK) {
                    break;
                }

                $current = $fromFirst ? $currentNode['next'] : $currentNode['previous'];
                $currentNode = $current !== false ? $this->getNode($current, $schemaTag) : null;
            } while ($current !== false && ($iterator < $limit || $limit == -1));
        }
    }

    /**
     * Clear double slashes in ways
     *
     * @param string $value
     *
     * @return string
     */
    static function clearDoubleSlashes(string $value): string
    {
        return self::replaceRecursive('//', '/', $value);
    }

    /**
     * Replace recursively in string
     *
     * @param string $search
     * @param string $replace
     * @param string $source
     *
     * @return mixed
     */
    static function replaceRecursive(string $search, string $replace, string $source): string
    {
        while (strpos($source, $search) !== false) {
            $source = str_replace($search, $replace, $source);
        }

        return $source;
    }

    /**
     * Add a inverse index of node
     *
     * @param string $nodeId
     * @param string $schema
     * @param string $index
     * @param string $wordSchema
     *
     * @return mixed
     */
    public function addInverseIndex(string|int|float $nodeId, string $schema, string $index, string $wordSchema): mixed
    {
        $node = $this->getNode("$nodeId." . DIV_NODES_INDEX_FILE_EXTENSION, $schema, [
            "indexes"     => [],
            "last_update" => date("Y-m-d h:i:s"),
        ]);

        $node['indexes'][$wordSchema] = $index;
        $node['last_update'] = date("Y-m-d h:i:s");
        $this->putNode("$nodeId." . DIV_NODES_INDEX_FILE_EXTENSION, $node, $schema);

        return $node;
    }

    /**
     * Wait for exclusive access to folder or file and do something
     *
     * @param string $path
     * @param \closure|string $action
     * @param int $max_execution_time
     * 
     * @return mixed
     * 
     * @throws \Exception
     */
    public function waitForUnlockAndDo(string $path, Closure|string $action, int $max_execution_time = 60)
    {
        $lockFile = $path;

        if (is_dir($path)) {
            $lockFile = $this->clearDoubleSlashes($path . '/' . DIV_NODES_LOCK_NODE);
        } else {
            $lockFile = $this->clearDoubleSlashes($path . DIV_NODES_LOCK_NODE);
            self::trash($this->getInstanceId(), function () use ($lockFile) {
                @unlink($lockFile);
            });
        }

        if (!file_exists($lockFile)) {
            $fp = fopen($lockFile, 'w');
            fclose($fp);
        }

        $fp = fopen($lockFile, 'r+');
        if (!$fp) {
            throw new Exception("Unable to open lock file: {$lockFile}");
        }

        $startTime = time();
        $timeoutOccurred = false;

        while (!flock($fp, LOCK_EX)) {
            usleep(100000);
            if ((time() - $startTime) > $max_execution_time) {
                $timeoutOccurred = true;
                break;
            }
        }

        if ($timeoutOccurred) {
            fclose($fp);
            throw new \Exception("Timeout exceeded while waiting for lock");
        }

        $remainingTime = $max_execution_time - (time() - $startTime);
        if ($remainingTime <= 0) {
            flock($fp, LOCK_UN);
            fclose($fp);
            throw new \Exception("No time left for operation after acquiring lock");
        }

        set_time_limit($remainingTime);

        try {
            $result = $action();
        } finally {
            flock($fp, LOCK_UN);
            fclose($fp);
        }

        return $result;
    }

    /**
     * Wait for exclusive folder access and do something
     *
     * @return mixed
     * @throws \Exception
     *
     */
    public function waitAndDo(string $folder, Closure|string $action, int $max_execution_time = 60): mixed
    {
        return $this->waitForUnlockAndDo($folder, $action, $max_execution_time);
    }

    /**
     * Wait for exclusive access to node, and do a closure
     *
     * @return mixed
     * @throws \Exception
     *
     */
    public function waitForNodeAndDo(string $nodeId, Closure|string $action, int $maxExecutionTime = 60, ?string $schema = null)
    {
        $schema = $schema ?? $this->schema;
        $filePath = DIV_NODES_ROOT . "{$schema}/{$nodeId}";

        return $this->waitForUnlockAndDo($filePath, $action, $maxExecutionTime);
    }

    /**
     * Save trash operation
     *
     * @param string  $instanceId
     * @param \closure $closure
     * @param array   $params
     */
    private static function trash(string $instanceId, Closure $closure, array $params = []): void
    {
        if (!isset(self::$__trash[$instanceId])) {
            self::$__trash[$instanceId] = [];
        }

        self::$__trash[$instanceId][] = [
            'f' => $closure,
            'p' => $params,
        ];
    }

    /**
     * Execute trash operations
     *
     * @param string $instanceId
     */
    public static function emptyTrash(string $instanceId): void
    {
        self::log("Empty trash for instance: $instanceId");

        if (!isset(self::$__trash[$instanceId])) {
            self::$__trash[$instanceId] = [];
        }

        foreach (self::$__trash[$instanceId] as $t) {
            $result = $t['f']($t['p']);
            self::log(" -> Executed trash operation: $result");
        }

        self::$__trash[$instanceId] = [];
    }

    /**
     * Check if a filename is valid
     * 
     * @param string|int|float $filename
     * 
     * @return bool
     */
    public function isValidId(string|int|float $id): bool
    {
        $id = "$id";
        foreach (DIV_NODES_INVALID_FILENAME_CHARS as $char) {
            if (strpos($id, $char) !== false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Generate UUID version 4
     * 
     * @return string
     */
    public static function generateUUIDv4(): string
    {
        // Generate 36 random hex characters (144 bits) 
        $uuid = bin2hex(random_bytes(18));

        // Insert dashes to match the UUID format 
        $uuid[8] = $uuid[13] = $uuid[18] = $uuid[23] = '-';

        // Set the UUID version to 4 
        $uuid[14] = '4';

        // Set the UUID variant: the 19th char must be in [8, 9, a, b] 
        $uuid[19] = [
            '8', '9', 'a', 'b', '8', '9',
            'a', 'b', 'c' => '8', 'd' => '9',
            'e' => 'a', 'f' => 'b'
        ][$uuid[19]] ?? $uuid[19];

        return $uuid;
    }

    /**
     * Destructor
     */
    public function __destruct()
    {
        // execute trash operations
        self::emptyTrash($this->getInstanceId());
    }
}
