<?php

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
 * @package com.divengine.nodes
 * @author  Rafa Rodriguez [@rafageist] <rafageist@hotmail.com>
 * @version 1.4
 *
 * @link    http://divengine.github.io
 * @link    https://divengine.github.io/div-nodes
 * @link    https://github.com/divengine/div-nodes
 */

/* CONSTANTS */
if (!defined("DIV_NODES_ROOT")) define("DIV_NODES_ROOT", "./");
if (!defined("DIV_NODES_LOG_FILE")) define("DIV_NODES_LOG_FILE", DIV_NODES_ROOT . "/divNodes.log");

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

/**
 * Class divNodes
 */
class divNodes
{
    static $__log_mode = false;
    static $__log_file = DIV_NODES_LOG_FILE;
    static $__version = 1.4;

    private $__instance_id = null;
    private $__triggers = [];

    static $__trash = [];
    static $__global_thread_id = null;

    var $schema = null;


    /**
     * Constructor
     *
     * @param string $schema
     */
    public function __construct($schema)
    {
        $this->setSchema($schema);
        self::log("New instance " . $this->getInstanceId() . " - THREAD ID: " . $this->getThreadID() . " - schema: $schema");
    }

    /**
     * Get current version
     *
     * @return float
     */
    public function getVersion()
    {
        return self::$__version;
    }

    /**
     * Return the ID of current instance (generate it if not exists)
     *
     * @return string
     */
    public function getInstanceId()
    {
        if (is_null($this->__instance_id))
            $this->__instance_id = uniqid(date("Ymdhis"), true);

        return $this->__instance_id;
    }

    /**
     * Return global proccess id
     * @return null|string
     */
    static function getGlobalThreadId()
    {
        if (is_null(self::$__global_thread_id)) {
            // getmypid(): Process IDs are not unique, thus they are a weak entropy source.
            // We recommend against relying on pids in security-dependent contexts.

            //... generate an id for current PHP proccess
            self::$__global_thread_id = uniqid(date("Ymdhis"), true);
        }

        return self::$__global_thread_id;
    }

    /**
     * Return current thread ID
     *
     * @return null|string
     */
    public function getThreadID()
    {
        return self::getGlobalThreadId();
    }

    /**
     * Protect some IDs in schemas
     *
     * @param $id
     *
     * @return bool
     */
    protected final function isReservedId($id)
    {
        return $id == '.references' || $id == '.index' || $id == '.stats' || $id == '.first' || $id == '.last' || $id == '.lock' || $id == '.queue' || $id == '.schema';
    }

    /**
     * Set the schema of work
     *
     * @param string $schema
     */
    public function setSchema($schema)
    {
        $this->addSchema($schema);
        $this->schema = $schema;
    }

    /**
     * Add schema
     *
     * @param string $schema
     */
    public function addSchema($schema)
    {
        @mkdir(self::clearDoubleSlashes(DIV_NODES_ROOT . "/" . $schema), 0777, true);
    }

    /**
     * Rename a schema
     *
     * @param string $schema
     * @param string $new_name
     *
     * @return boolean
     */
    public function renameSchema($new_name, $schema)
    {
        if (is_null($schema)) {
            $schema = $this->schema;
        }

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
     * @return boolean
     */
    public function existsSchema($schema = null)
    {
        if (is_null($schema)) {
            $schema = $this->schema;
        }

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
     */
    static function logOn()
    {
        self::$__log_mode = true;
    }

    /**
     * Switch logs to OFF
     */
    static function logOff()
    {
        self::$__log_mode = false;
    }

    /**
     * Log messages
     *
     * @param string $message
     * @param string $level
     */
    static function log($message, $level = "INFO")
    {
        if (self::$__log_mode) {
            $message = date("Y-m-d h:i:s") . ' - ' . self::getGlobalThreadId() . " - [$level] $message \n";
            $f = fopen(self::$__log_file, 'a');
            fputs($f, $message);
            fclose($f);
        }
    }

    /**
     * Remove a schema
     *
     * @param string $schema
     * @param boolean $ofNodes
     *
     * @return boolean
     */
    public function delSchema($schema, $ofNodes = true)
    {
        if (file_exists(DIV_NODES_ROOT . $schema)) {
            if (!is_dir(DIV_NODES_ROOT . $schema)) {
                return false;
            }
            $dir = scandir(DIV_NODES_ROOT . $schema);
            //echo DIV_NODES_ROOT . $schema. " == ".count($dir)."\n";
            foreach ($dir as $entry) {
                if ($entry != "." && $entry != "..") {
                    if (is_dir(DIV_NODES_ROOT . $schema . "/$entry")) {
                        if ($entry == ".queue" || $entry == ".schema")
                            $this->delSchema($schema . "/$entry", false);
                        else
                            $this->delSchema($schema . "/$entry", $ofNodes);

                    } else {
                        //echo "--- CHECK ".DIV_NODES_ROOT . $schema . "/$entry".($ofNodes?"-- OF NODES --":"")."\n";
                        if ($ofNodes) {
                            if (!$this->isReservedId($entry)) {
                                $this->delNode($entry, $schema);
                            }
                        } else {
                            //echo "UNLINK ".DIV_NODES_ROOT . $schema . "/$entry"."\n";
                            @unlink(DIV_NODES_ROOT . $schema . "/$entry");
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

                // If the schema of reference is a subschema of this schema
                if ($schema == substr($sch, 0, strlen($schema))) {
                    continue;
                }

                $relats = $this->getReferences($sch);
                $new_references = [];
                foreach ($relats as $re) {
                    if ($re['schema'] != $schema && $re['foreign_schema'] != $schema) {
                        $new_references[] = $re;
                    }
                }
                file_put_contents(self::clearDoubleSlashes(DIV_NODES_ROOT . "/$sch/.references"), serialize($new_references));
            }

            @unlink(self::clearDoubleSlashes(DIV_NODES_ROOT . "/$schema/.references"));
            @unlink(self::clearDoubleSlashes(DIV_NODES_ROOT . "/$schema/.stats"));
            @rmdir(self::clearDoubleSlashes(DIV_NODES_ROOT . "/$schema"));

            return true;
        }

        return false;
    }

    /**
     * Remove one node
     *
     * @param string $id
     * @param string $schema
     *
     * @return boolean
     */
    public function delNode($id, $schema = null)
    {
        if (is_null($schema)) $schema = $this->schema;
        if (!$this->existsSchema($schema)) return false;
        $fullNodePath = DIV_NODES_ROOT . "$schema/$id";
        if (!file_exists($fullNodePath)) return false;

        $is_order = false;

        if (file_exists(DIV_NODES_ROOT . $schema . "/.first")
            && file_exists(DIV_NODES_ROOT . $schema . "/.last")
            && !$this->isReservedId($id)) {
            $is_order = true;
        }

        if ($is_order)
            return $this->waitAndDo(DIV_NODES_ROOT . "$schema/.queue/.schema", function (divNodes $db, $params) {
                $schema = $params['schema'];
                $fullNodePath = $params['fullNodePath'];
                $id = $params['id'];

                $raw_data = file_get_contents($fullNodePath);
                $node = @unserialize($raw_data);
                if ($node === false) $node = $raw_data;

                $r = $db->executeTriggers(DIV_NODES_TRIGGER_BEFORE_DEL, $node, $id, $schema, null, $node);
                if ($r === DIV_NODES_ROLLBACK_TRANSACTION) {
                    return DIV_NODES_ROLLBACK_TRANSACTION;
                }

                // TODO: here or after unlinks? Si es despues hay q tener en cuenta si es raw data

                $r = $db->executeTriggers(DIV_NODES_TRIGGER_AFTER_DEL, $node, $id, $schema, $node, $node);

                if ($r === DIV_NODES_ROLLBACK_TRANSACTION) {
                    return DIV_NODES_ROLLBACK_TRANSACTION;
                }

                // check if is order node (double linked node)
                if (isset($node['previous']) && isset($node['next'])) {
                    if ($node['previous'] === false && $node['next'] === false) // is the only one
                    {
                        $db->setOrderFirst($schema, '');
                        $db->setOrderLast($schema, '');
                    }

                    if ($node['previous'] === false && $node['next'] !== false) // is the first
                    {
                        // update the new first
                        $next = $db->getNode($node['next'], $schema);
                        $next['previous'] = false;
                        $db->putNode($node['next'], $next, $schema);
                        $db->setOrderFirst($schema, $node['next']);
                    }

                    if ($node['next'] === false && $node['previous'] !== false) // is the last
                    {
                        // update the new last
                        $previous = $db->getNode($node['previous'], $schema);
                        $previous['next'] = false;
                        $db->putNode($node['previous'], $previous, $schema);
                        $db->setOrderLast($schema, $node['previous']);
                    }

                    if ($node['next'] !== false && $node['previous'] !== false) // is in the middle
                    {
                        $previous = $db->getNode($node['previous'], $schema);
                        $next = $db->getNode($node['next'], $schema);
                        $previous['next'] = $node['next'];
                        $next['previous'] = $node['previous'];
                        $db->putNode($node['next'], $next, $schema);
                        $db->putNode($node['previous'], $previous, $schema);
                    }
                }

                // Delete the node
                unlink(self::clearDoubleSlashes(DIV_NODES_ROOT . "/$schema/$id"));

                // Delete indexes
                $idx_path = DIV_NODES_ROOT . "/$schema/$id.idx";
                if (file_exists($idx_path)) {
                    $idx = unserialize(file_get_contents($idx_path));

                    if (isset($idx['indexes'])) foreach ($idx['indexes'] as $word_schema => $index_id) {
                        $db->delNode($index_id, $word_schema);
                    }

                    unlink(self::clearDoubleSlashes($idx_path));
                }

                // record stats
                if (!(pathinfo($fullNodePath, PATHINFO_EXTENSION) == "idx"
                    && file_exists(substr($fullNodePath, 0, strlen($fullNodePath) - 4)))) {
                    $db->changeStats('{count} -= 1', $schema);
                }

                // DO NOT DELETE HERE THE QUEUE FOLDER !! refer to waitForNodeAndDo

                return true;
            }, [
                "schema" => $schema,
                "fullNodePath" => $fullNodePath,
                "id" => $id
            ], 60, null, $schema);
        else
            return $this->waitForNodeAndDo($id, function (divNodes $db, $params) {
                $schema = $params['schema'];
                $fullNodePath = $params['fullNodePath'];
                $id = $params['id'];

                $raw_data = file_get_contents($fullNodePath);
                $node = @unserialize($raw_data);
                if ($node === false) $node = $raw_data;

                $r = $db->executeTriggers(DIV_NODES_TRIGGER_BEFORE_DEL, $node, $id, $schema, null, $node);
                if ($r === DIV_NODES_ROLLBACK_TRANSACTION) {
                    return DIV_NODES_ROLLBACK_TRANSACTION;
                }

                $restore = [];

                // Delete cascade
                $references = $db->getReferences($schema);
                foreach ($references as $rel) {
                    if ($rel['foreign_schema'] == $schema) {
                        if (!$db->existsSchema($rel['schema'])) continue;

                        $ids = $db->getNodesID($rel['schema']);
                        foreach ($ids as $fid) {
                            $referencedNode = $db->getNode($fid, $rel['schema']);
                            $restore[] = [
                                "node" => $referencedNode,
                                "id" => $fid,
                                "schema" => $rel['schema']
                            ];

                            $delete_node = false;

                            if (is_array($referencedNode)) {
                                if (isset($referencedNode[$rel['property']])) {
                                    if ($referencedNode[$rel['property']] == $id) {
                                        if ($rel['delete_cascade'] == true) {
                                            $delete_node = true;
                                        } else {
                                            $db->setNode($fid, [
                                                $rel['property'] => null
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
                                            $db->setNode($fid, [
                                                $rel['property'] => null
                                            ], $rel['schema']);
                                        }
                                    }
                                }
                            }

                            if ($delete_node) {
                                $r = $db->delNode($fid, $rel['schema']);
                                if ($r === DIV_NODES_ROLLBACK_TRANSACTION) {
                                    return DIV_NODES_ROLLBACK_TRANSACTION;
                                }
                            }
                        }
                    }
                }

                // Delete the node
                unlink(self::clearDoubleSlashes(DIV_NODES_ROOT . "/$schema/$id"));

                $r = $db->executeTriggers(DIV_NODES_TRIGGER_AFTER_DEL, $node, $id, $schema, $node, $node);

                if ($r === DIV_NODES_ROLLBACK_TRANSACTION) {
                    foreach ($restore as $rest) {
                        if ($db->existsNode($rest['id'], $rest['schema'])) {
                            $db->setNode($rest['id'], $rest['node'], $rest['schema']);
                        } else {
                            $db->addNode($rest['node'], $rest['id'], $rest['schema']);
                        }
                    }

                    return DIV_NODES_ROLLBACK_TRANSACTION;
                }

                // Delete indexes
                $idx_path = DIV_NODES_ROOT . "/$schema/$id.idx";
                if (file_exists($idx_path)) {
                    $idx = unserialize(file_get_contents($idx_path));

                    if (isset($idx['indexes'])) foreach ($idx['indexes'] as $word_schema => $index_id) {
                        $db->delNode($index_id, $word_schema);
                    }

                    unlink(self::clearDoubleSlashes($idx_path));
                }

                // record stats
                if (!(pathinfo($fullNodePath, PATHINFO_EXTENSION) == "idx"
                    && file_exists(substr($fullNodePath, 0, strlen($fullNodePath) - 4)))) {
                    $db->changeStats('{count} -= 1', $schema);
                }

                // DO NOT DELETE HERE THE QUEUE FOLDER !! refer to waitForNodeAndDo

                return true;
            }, [
                "schema" => $schema,
                "fullNodePath" => $fullNodePath,
                "id" => $id
            ], 60, $schema);
    }

    /**
     * Return a list of schema's references
     *
     * @param string $schema
     *
     * @return array
     */
    public function getReferences($schema = null)
    {
        if (is_null($schema)) $schema = $this->schema;

        if (!$this->existsSchema($schema)) return [];

        $path = DIV_NODES_ROOT . $schema . "/.references";
        if (!file_exists($path)) file_put_contents($path, serialize([]));

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
    public function getNodesID($schema = null)
    {
        if (is_null($schema)) {
            $schema = $this->schema;
        }

        if (!$this->existsSchema($schema)) {
            return false;
        }

        $list = [];
        $dir = scandir(DIV_NODES_ROOT . $schema);

        foreach ($dir as $entry) {
            $full_path = DIV_NODES_ROOT . $schema . "/$entry";
            if (!is_dir($full_path)) {
                if (!$this->isReservedId($entry)) {
                    if (pathinfo($full_path, PATHINFO_EXTENSION) == "idx" && file_exists(substr($full_path, 0, strlen($full_path) - 4))) continue;
                    $list[] = $entry;
                }
            }
        }

        return $list;
    }

    /**
     * Return a node
     *
     * @param mixed $id
     * @param string $schema
     * @param mixed $default
     * @param boolean $keepLocked
     *
     * @return mixed
     */
    public function getNode($id, $schema = null, $default = null, $keepLocked = false)
    {

        if (is_null($schema)) $schema = $this->schema;

        // read pure data
        $node_path = self::clearDoubleSlashes(DIV_NODES_ROOT . "/$schema/$id");

        return $this->waitForNodeAndDo($id, function ($db, $params) {
            //echo "GET NODE: ".$params['node_path']."\n";

            // ... and load
            if (is_null($params['default'])) $data = file_get_contents($params['node_path']);
            else {
                // hide errors if not exists
                $data = @file_get_contents($params['node_path']);
            }

            if ($data === false) // the node not exists ...
            {
                return $params['default'];
            }

            $node = unserialize($data);

            return $node;

        }, [
            "node_path" => $node_path,
            "default" => $default,
            "schema" => $schema,
            "keepLocked" => $keepLocked
        ], 60, $schema);

    }

    /**
     * Update data of a node
     *
     * @param mixed $id
     * @param mixed $data Any data or closure function
     * @param string $schema
     * @param boolean $cop
     * @param array $params
     *
     * @return mixed
     */
    public function setNode($id, $data, $schema = null, $cop = true, $params = [])
    {
        if (is_null($schema)) $schema = $this->schema;

        if (!$this->existsSchema($schema)) return false;

        $setter = null;

        if (is_callable($data)) {
            $setter = $data;
            $data = null;
        }

        return $this->waitForNodeAndDo($id, function (divNodes $db, $params) {

            $id = $params['id'];
            $data = $params['data'];
            $schema = $params['schema'];
            $setter = $params['setter'];
            $cop = $params['cop'];

            //echo "SET NODE ".DIV_NODES_ROOT . "$schema/$id\n";

            if ($db->existsNode($id, $schema)) {

                $raw_data = file_get_contents(DIV_NODES_ROOT . "$schema/$id");
                $node = unserialize($raw_data);
                if ($node == false) $node = $raw_data;
            } // $db->getNode($id, $schema);
            else $node = $data;

            $r = $db->executeTriggers(DIV_NODES_TRIGGER_BEFORE_SET, $id, $node, $schema, null, $data);
            if ($r === DIV_NODES_ROLLBACK_TRANSACTION) return DIV_NODES_ROLLBACK_TRANSACTION;

            $old = $node;
            if (!is_null($setter)) $data = $setter($node, $this, $params['params']);

            if ($cop) $node = $db->cop($node, $data); // update the node
            else $node = $data; // replace node

            file_put_contents(DIV_NODES_ROOT . "$schema/$id", serialize($node));

            $r = $db->executeTriggers(DIV_NODES_TRIGGER_AFTER_SET, $id, $node, $schema, $old, $data);

            if ($r === DIV_NODES_ROLLBACK_TRANSACTION) file_put_contents(DIV_NODES_ROOT . "$schema/$id", serialize($old));

            return $node;
        }, [
            "id" => $id,
            "schema" => $schema,
            "data" => $data,
            "setter" => $setter,
            "cop" => $cop,
            "params" => $params
        ], 60, $schema);
    }

    /**
     * Replace node
     *
     * @param        $id
     * @param        $data
     * @param string $schema
     * @param array $params
     *
     * @return mixed
     */
    public function putNode($id, $data, $schema = null, $params = [])
    {
        return $this->setNode($id, $data, $schema, false, $params);
    }

    /**
     * Complete object/array properties
     *
     * @param mixed $source
     * @param mixed $complement
     * @param integer $level
     *
     * @return mixed
     */
    final static function cop(&$source, $complement, $level = 0)
    {
        $null = null;

        if (is_null($source)) return $complement;
        if (is_null($complement)) return $source;
        if (is_scalar($source) && is_scalar($complement)) return $complement;
        if (is_scalar($complement) || is_scalar($source)) return $source;

        if ($level < 100) { // prevent infinite loop
            if (is_object($complement)) {
                $complement = get_object_vars($complement);
            }

            foreach ($complement as $key => $value) {
                if (is_object($source)) {
                    if (isset($source->$key)) {
                        $source->$key = self::cop($source->$key, $value, $level + 1);
                    } else {
                        $source->$key = self::cop($null, $value, $level + 1);
                    }
                }
                if (is_array($source)) {
                    if (isset($source[$key])) {
                        $source[$key] = self::cop($source[$key], $value, $level + 1);
                    } else {
                        $source[$key] = self::cop($null, $value, $level + 1);
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
    public function existsNode($id, $schema = null)
    {
        if (is_null($schema)) $schema = $this->schema;

        $fullPath = DIV_NODES_ROOT . $schema . "/$id";

        self::log("->existsNode($id, $schema): $fullPath");

        if (file_exists($fullPath)) {
            if (!is_dir($fullPath)) return true;
            self::log("---> $fullPath is a folder");
        }

        return false;
    }

    /**
     * Insert a node in schema
     *
     * @param mixed $node
     * @param string $id
     * @param string $schema
     *
     * @return mixed
     */
    public function addNode($node, $id = null, $schema = null)
    {
        if (is_null($schema)) $schema = $this->schema;
        if (is_null($id)) $id = date("Ymdhis") . uniqid();

        if ($this->isReservedId($id)) {
            self::log("Invalid ID '$id' for node");
            return false;
        }

        if ($this->existsNode($id, $schema)) {
            self::log("Node ID '$id' already exists");
            return false;
        }

        $node = $this->executeTriggers(DIV_NODES_TRIGGER_BEFORE_ADD, $node, $id, $schema, null, $node);
        if ($node == false || $node == DIV_NODES_ROLLBACK_TRANSACTION)
            return DIV_NODES_ROLLBACK_TRANSACTION;

        // save node
        $data = serialize($node);
        file_put_contents(DIV_NODES_ROOT . $schema . "/$id", $data);

        // record the stats
        $full_path = DIV_NODES_ROOT . $schema . "/$id";
        if (!(pathinfo($full_path, PATHINFO_EXTENSION) == "idx" && file_exists(substr($full_path, 0, strlen($full_path) - 4)))) {
            $this->changeStats('{count} += 1', $schema);
        }

        $r = $this->executeTriggers(DIV_NODES_TRIGGER_AFTER_ADD, $node, $id, $schema, null, $node);

        if ($r === DIV_NODES_ROLLBACK_TRANSACTION) {

            $this->delNode($id, $schema);
            return DIV_NODES_ROLLBACK_TRANSACTION;
        }

        return $id;
    }

    /**
     * Add a trigger
     *
     * @param $event
     * @param $callable
     */
    public function addTrigger($event, $callable)
    {
        if (!isset($this->__triggers[$event]))
            $this->__triggers[$event] = [];

        $this->__triggers[$event][] = $callable;
    }

    /**
     * Execute triggers
     *
     * @param string $event
     * @param mixed $node
     * @param string $id
     * @param string $schema
     * @param mixed $old
     * @param mixed $data
     *
     * @return mixed
     */
    public function executeTriggers($event, $node, $id, $schema = null, $old = null, $data = null)
    {
        if (is_null($schema))
            $schema = $this->schema;

        if (!isset($this->__triggers[$event]))
            $this->__triggers[$event] = [];

        foreach ($this->__triggers[$event] as $f) {
            $result = $f($node, $id, $schema, $old, $data);

            if ($result == DIV_NODES_ROLLBACK_TRANSACTION)
                return DIV_NODES_ROLLBACK_TRANSACTION;

            $node = $result;
        }

        return $node;
    }

    /**
     * Return a list of nodes recursively
     *
     * @param string $schema
     * @param array $paramsBySchema Apply this params by schema
     * @param array $paramsDefault Default params by schema
     * @param integer $offset From offset
     * @param integer $limit Limit the result
     * @param bool $onlyIds Return only IDs, not the nodes
     *
     * @return array
     */
    public function getRecursiveNodes($schema = "/", $paramsBySchema = [], $paramsDefault = [], $offset = 0, $limit = -1, $onlyIds = false)
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

            if ($list !== false) $nodes[$schema] = $list;
        }

        // limit result
        $list = [];
        $i = 0;
        $c = 0;
        foreach ($nodes as $schema => $ids) {
            foreach ($ids as $id) if ($i >= $offset) {
                if ($c < $limit || $limit == -1) {
                    if (!isset($list[$schema])) {
                        $list[$schema] = [];
                    }
                    if (!$onlyIds) {
                        $list[$schema][$id] = $this->getNode($id, $schema);
                    } else {
                        $list[$schema][$id] = $id;
                    }
                    $c++;
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
    public function getSchemas($from)
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

                    if ($entry != '.' && $entry != '..' && !is_file(DIV_NODES_ROOT . $fullSchema)
                        && $entry != ".queue" && $entry != ".schema") {
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
     * @param array $params
     * @param string $schema
     *
     * @return mixed
     */
    public function getNodes($params = [], $schema = null, $onlyIds = false)
    {
        if (is_null($schema)) $schema = $this->schema;
        if (!$this->existsSchema($schema)) return false;

        $dp = [
            "offset" => 0,
            "limit" => null,
            "fields" => "*",
            "order" => null,
            "order_asc" => true
        ];

        $params = self::cop($dp, $params);

        $data = [
            'params' => $params,
            'ids' => [],
            'list' => []
        ];


        // CASE 1: where, not order, limit

        if (isset($params['where']))
            $this->forEachNode(function ($node, $file, $schema, $db, &$otherData, $iterator) {

                // if no order...
                if (!isset($otherData['params']['order']) || (isset($otherData['params']['order'])
                        && ($otherData['params']['order'] === false || !is_null($otherData['params']['order']))))
                    // ..check for offset and limit
                    if ($iterator < $otherData['params']['offset'] || ($otherData['params']['limit'] <= 0
                            && !is_null($otherData['params']['limit'])))
                        return DIV_NODES_FOR_CONTINUE_DISCARDING;

                $vars = [];
                if (is_object($node)) $vars = get_object_vars($node);
                elseif (is_array($node)) $vars = $node;
                elseif (is_scalar($node)) $vars = ['value' => $node];

                $w = $otherData['params']['where'];

                foreach ($vars as $key => $value)
                    $w = str_replace('{' . $key . '}', '$vars["' . $key . '"]', $w);

                $w = str_replace('{id}', '$id', $w);

                $r = false;
                $st = '$r = ' . $w . ';';
                eval($st);

                if ($r === true) {
                    $otherData['ids'][] = $file;
                    $otherData['list'][$file] = $node;
                }

                $otherData['params']['limit']--;

            }, null, $data);

        // CASE 2: not where, not order, limit
        elseif (!isset($params['order']) || (isset($params['order']) && ($params['order'] === false || is_null($params['order']))))
            $this->forEachNode(function ($node, $file, $schema, $db, &$otherData, $iterator) {

                // get nodes without order
                if ($iterator >= $otherData['params']['offset'] && ($otherData['params']['limit'] > 0 || is_null($otherData['params']['limit']))) {
                    $otherData['ids'][] = $file;
                    $otherData['list'][$file] = $node;
                }

                if (!is_null($otherData['params']['limit']))
                    $otherData['params']['limit']--;

            }, null, $data);

        // CASE 3: not where, order + limit (get all for sort after)
        else
            $this->forEachNode(function ($node, $file, $schema, $db, &$otherData, $iterator) {
                $otherData['ids'][] = $file;
                $otherData['list'][$file] = $node;
            }, null, $data);

        $newIds = $data['ids'];

        // sort results
        if (isset($params['order'])) {
            $order = $params['order'];

            if ($order !== false && !is_null($order)) {
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
                    if ($c < $params['limit'] || is_null($params['limit'])) {
                        $list[] = $id;
                        $c++;
                    }
                }
                $i++;
            }

            $newIds = $list;
        }

        if ($onlyIds) return $newIds;


        $list = [];
        foreach ($newIds as $id) $list[$id] = $data['list'][$id];

        return $list;
    }

    /**
     * Return the count of nodes
     *
     * @param array $params
     * @param string $schema
     *
     * @return integer
     */
    public function getCount($params = [], $schema = null)
    {
        if (is_null($schema)) {
            $schema = $this->schema;
        }

        if (!$this->existsSchema($schema)) {
            return false;
        }

        $dp = [
            "where" => "true"
        ];
        $params = self::cop($dp, $params);

        $ids = $this->getNodesID($schema);
        $list = [];

        $c = 0;
        foreach ($ids as $id) {
            $node = $this->getNode($id, $schema);

            if (is_object($node)) {
                $vars = get_object_vars($node);
            } elseif (is_array($node)) {
                $vars = $node;
            } elseif (is_scalar($node)) {
                $vars = [
                    'value' => $node
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
     * @param array $params
     * @param string $schema
     *
     * @return boolean
     */
    public function delNodes($params = [], $schema = null)
    {
        if (is_null($schema)) {
            $schema = $this->schema;
        }

        if (!$this->existsSchema($schema)) {
            return false;
        }

        $dp = [
            "where" => "true",
            "offset" => 0,
            "limit" => -1
        ];
        $params = self::cop($dp, $params);

        if ($params['where'] != "true") {
            $nodes = $this->getNodes($params, $schema);
            foreach ($nodes as $id => $node) {
                $this->delNode($id, $schema);
            }
        } else {
            $nodes = $this->getNodesID($schema);
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
    public function setNodeID($oldId, $newId, $schema = null)
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
    public function addReference($params = [])
    {
        $dp = [
            "schema" => $this->schema,
            "foreign_schema" => $this->schema,
            "update_cascade" => true,
            "delete_cascade" => true
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
        $freferences = $this->getReferences($foreign_schema);

        foreach ($references as $rel) {
            if (serialize($rel) == serialize($params)) {
                return true;
            }
        }

        $references[] = $params;
        $freferences[] = $params;

        file_put_contents(DIV_NODES_ROOT . $schema . "/.references", serialize($references));
        file_put_contents(DIV_NODES_ROOT . $foreign_schema . "/.references", serialize($freferences));

        return true;
    }

    /**
     * Delete a reference
     *
     * @param array $params
     *
     * @return boolean
     */
    public function delReference($params = [])
    {
        $dp = [
            "schema" => $this->schema,
            "foreign_schema" => $this->schema,
            "update_cascade" => true,
            "delete_cascade" => true
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
     * @param closure $closure
     * @param string $schema
     * @param array $otherData
     */
    public function forEachNode($closure, $schema = null, &$otherData = [])
    {
        $iterator = 0;
        if (is_null($schema)) $schema = $this->schema;

        if ($dir = opendir(DIV_NODES_ROOT . $schema)) {
            while (($file = readdir($dir)) !== false) {
                $full_path = DIV_NODES_ROOT . $schema . "/" . $file;

                if (pathinfo($full_path, PATHINFO_EXTENSION) == "idx" && file_exists(substr($full_path, 0, strlen($full_path) - 4))) continue;

                if (!$this->isReservedId($file) && $file != "." && $file != ".." && !is_dir($full_path)) {
                    $node = $this->getNode($file, $schema);
                    $md5 = md5(serialize($node));

                    $result = $closure($node, $file, $schema, $this, $otherData, $iterator);

                    if ($result == DIV_NODES_FOR_BREAK) break;
                    if ($result == DIV_NODES_FOR_CONTINUE_DISCARDING) continue;

                    // default: DIV_NODES_FOR_CONTINUE_SAVING)

                    $new_md5 = md5(serialize($node));
                    if ($md5 != $new_md5) if ($result == DIV_NODES_FOR_REPLACE_NODE) $this->putNode($file, $node, $schema);
                    else
                        $this->setNode($file, $node, $schema);

                    $iterator++;
                }
            }
            closedir($dir);
        }
    }

    /**
     * Get words from content
     *
     * @param mixed $content
     * @param string $chars
     *
     * @return array
     */
    public function getWords($content, $chars = ' abcdefghijklmnopqrstuvwxyz1234567890')
    {
        $content = "$content";

        $l = strlen($content);

        $new_content = '';
        for ($i = 0; $i < $l; $i++) if (stripos($chars, $content[$i]) !== false) $new_content .= $content[$i];
        else $new_content .= ' ';

        $new_content = trim(strtolower($new_content));

        while (strpos($new_content, '  ') !== false) $new_content = str_replace('  ', ' ', $new_content);

        $words = explode(' ', $new_content);

        $new_words = [];
        foreach ($words as $word) $new_words[$word] = $word;

        return $new_words;
    }

    /**
     * Add index of node
     *
     * @param array $words
     * @param string $nodeId
     * @param string $schema
     * @param null $indexSchema
     * @param boolean $wholeWords
     */
    public function addIndex($words, $nodeId, $schema = null, $indexSchema = null, $wholeWords = false)
    {
        if (is_null($schema)) $schema = $this->schema;
        if (is_null($indexSchema)) $indexSchema = $schema . '/.index';

        $this->addSchema($indexSchema);

        $pathToNode = "$schema/$nodeId";
        $id = md5($pathToNode);

        foreach ($words as $word) {
            $l = strlen($word);
            if ($wholeWords) $wordSchema = $word;
            else {
                $wordSchema = '';
                for ($i = 0; $i < $l; $i++) $wordSchema .= $word[$i] . '/';
            }

            $wordSchema = "$indexSchema/$wordSchema";

            $this->addSchema($wordSchema);

            $node = $this->getNode($id, $wordSchema, [
                "schema" => $schema,
                "id" => $nodeId,
                "path" => $pathToNode,
                "last_update" => date("Y-m-d h:i:s")
            ]);

            if ($this->existsNode($id, $wordSchema)) $this->setNode($id, $node, $wordSchema);
            else
                $this->addNode($node, $id, $wordSchema);

            $this->addInverseIndex($nodeId, $schema, $id, $wordSchema);
        }
    }

    /**
     * Create index of schema
     *
     * @param closure $contentExtractor
     * @param string $schema
     * @param string $indexSchema
     * @param boolean $wholeWords
     */
    public function createIndex($contentExtractor = null, $schema = null, $indexSchema = null, $wholeWords = false)
    {
        if (is_null($contentExtractor)) $contentExtractor = function ($node, $nodeId) {
            $content = '';

            if (is_object($node)) {
                if (method_exists($node, '__toContent')) $content = $node->__toContent();
                elseif (method_exists($node, '__toString')) $content = "$node";
            } elseif (is_scalar($node)) $content = "$node";

            return $content;
        };

        $otherData = [
            'indexSchema' => $indexSchema,
            'contentExtractor' => $contentExtractor,
            'wholeWords' => $wholeWords
        ];

        // indexing each node
        $this->forEachNode(function ($node, $nodeId, $schema, divNodes $db, $otherData) {

            $contentExtractor = $otherData['contentExtractor'];
            $indexSchema = $otherData['indexSchema'];
            $words = [];
            $extract_words = true;

            // extract words from built-in node method
            if (is_object($node)) {
                if (method_exists($node, '__toWords')) {
                    $extract_words = false;
                    $words = $node->__toWords();
                }
            }

            // get content
            $content = $contentExtractor($node, $nodeId);

            // extract words
            if ($extract_words && count($words) == 0) $words = $db->getWords($content);

            $db->addIndex($words, $nodeId, $schema, $indexSchema, $otherData['wholeWords']);

        }, $schema, $otherData);
    }

    /**
     * Full text search
     *
     * @param string $phrase
     * @param string $indexSchema
     * @param int $offset
     * @param int $limit
     *
     * @return array
     */
    public function search($phrase, $indexSchema = null, $offset = 0, $limit = -1)
    {
        if (is_null($indexSchema)) $indexSchema = $this->schema . '/.index';

        $results = [];

        $words = $this->getWords($phrase);
        $words[$phrase] = $phrase;

        foreach ($words as $word) {
            // build schema from word
            $l = strlen($word);

            for ($wholeWords = 0; $wholeWords < 2; $wholeWords++) {
                $schema = '';
                if ($wholeWords == 0) for ($i = 0; $i < $l; $i++) $schema .= $word[$i] . "/";
                else
                    $schema = $word; // whole word

                $schema = "$indexSchema/$schema";

                if ($this->existsSchema($schema)) {
                    // get indexes
                    $schemas = $this->getRecursiveNodes($schema, [], [], $offset, $limit);

                    // calculate score
                    foreach ($schemas as $sch => $nodes) foreach ($nodes as $node) {
                        $id = md5($node['path']);

                        if (!isset($results[$id])) {
                            $node['score'] = 0;
                            $results[$id] = $node;
                        }

                        $results[$id]['score']++;
                    }
                }
            }
        }

        // sort results
        uasort($results, function ($a, $b) {
            if ($a['score'] == $b['score']) return 0;

            return $a['score'] > $b['score'] ? -1 : 1;
        });

        return $results;
    }

    /**
     * Default stats structure
     *
     * @return array
     */
    private function defaultStats()
    {
        return ['count' => 0];
    }

    /**
     * Return the stats of schema
     *
     * @param null $schema
     *
     * @return array|mixed
     */
    public function getStats($schema = null)
    {
        $stats = null;
        if (!$this->existsSchema($schema)) return null;
        if ($this->existsNode(".stats", $schema)) $stats = $this->getNode(".stats", $schema, null);

        if (is_null($stats)) $stats = $this->reStats($schema);

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
    public function changeStats($change, $schema = null)
    {
        if (is_null($schema)) $schema = $this->schema;

        return $this->setNode(".stats", function ($stats, divNodes $db, $params = []) {

            $change = $params['change'];

            if ($stats === false || is_null($stats) || empty($stats))
                $stats = $db->defaultStats();

            if (is_string($change)) {
                // change stats
                $expression = $change;
                foreach ($stats as $key => $value) $expression = str_replace('{' . $key . '}', '$stats["' . $key . '"]', $expression);
                @eval($expression . ";");
            } elseif (is_callable($change)) {
                $change($stats);
            }

            return $stats;
        }, $schema, true, [
            "change" => $change
        ]);
    }

    /**
     * Re-write stats of schema
     *
     * @param null $schema
     *
     * @return array
     */
    public function reStats($schema = null)
    {
        if (is_null($schema)) $schema = $this->schema;

        $stats = $this->defaultStats();

        // 'count' stat
        $this->forEachNode(function ($node, $file, $schema, $db, &$stats = []) {
            $stats['count']++;

            return DIV_NODES_FOR_CONTINUE_DISCARDING;
        }, $schema, $stats);

        // save stat
        $this->delNode('.stats', $schema);

        // no use addNode!
        file_put_contents(DIV_NODES_ROOT . $schema . "/.stats", serialize($stats));

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
    public function renameNode($oldId, $newId, $schema = null)
    {
        if (is_null($schema)) $schema = $this->schema;
        if ($this->existsNode($newId, $schema)) return false;
        if (!$this->existsNode($oldId, $schema)) return false;

        return $this->waitForNodeAndDo($oldId, function (divNodes $db, $params) {

            $oldId = $params['oldId'];
            $newId = $params['newId'];
            $schema = $params['schema'];

            // update references
            $restore = [];
            $references = $db->getReferences($schema);
            foreach ($references as $rel) {
                if ($rel['foreign_schema'] == $schema) {
                    if (!$db->existsSchema($rel['schema'])) continue;

                    $ids = $db->getNodesID($rel['schema']);

                    foreach ($ids as $fid) {
                        $node = $db->getNode($fid, $rel['schema']);

                        $restore[] = [
                            "node" => $node,
                            "id" => $fid,
                            "schema" => $rel['schema']
                        ];

                        if (is_array($node)) {
                            if (isset($node[$rel['property']])) {
                                if ($node[$rel['property']] == $oldId) {
                                    $db->setNode($fid, [
                                        $rel['property'] => $newId
                                    ], $rel['schema']);

                                }
                            }
                        } elseif (is_object($node)) {
                            if (isset($node->$rel['property'])) {
                                if ($node->$rel['property'] == $oldId) {
                                    $db->setNode($fid, [
                                        $rel['property'] => $newId
                                    ], $rel['schema']);
                                }
                            }
                        }
                    }
                }
            }

            // update indexes
            if (file_exists(DIV_NODES_ROOT . $schema . "/$oldId.idx")) {
                $idx = $db->getNode("$oldId.idx", $schema);

                foreach ($idx['indexes'] as $wordSchema => $index) {
                    // update index
                    $pathToNode = "$schema/$newId";
                    $nodeIndex = $this->getNode($index, $wordSchema);
                    $nodeIndex['id'] = $newId;
                    $nodeIndex['last_update'] = date("Y-m-d h:i:s");
                    $nodeIndex['path'] = $pathToNode;

                    $db->setNode($index, $nodeIndex, $wordSchema);

                    // rename index (recursive call)

                    $newIndex = md5($pathToNode);
                    $db->renameNode($index, $newIndex, $wordSchema);

                    // update inverse indexes
                    $idx['indexes'][$wordSchema] = $newIndex;
                }

                // update inverse indexes
                $idx["last_update"] = date("Y-m-d h:i:s");
                $this->putNode("$oldId.idx", $idx, $schema);

                // real rename of idx file
                rename(DIV_NODES_ROOT . $schema . "/$oldId.idx", DIV_NODES_ROOT . $schema . "/$newId.idx");
            }

            // update .queues
            $queueFile = DIV_NODES_ROOT . "$schema/.queue/$oldId";
            if (file_exists($queueFile)) {
                $queueFolder = file_get_contents($queueFile);
                file_put_contents(DIV_NODES_ROOT . "$schema/.queue/$queueFolder.idx", $newId);
                rename($queueFile, DIV_NODES_ROOT . "$schema/.queue/$newId");
            }

            // real rename of node file
            rename(DIV_NODES_ROOT . $schema . "/$oldId", DIV_NODES_ROOT . $schema . "/$newId");

            return true;
        }, [
            "oldId" => $oldId,
            "newId" => $newId,
            "schema" => $schema
        ]);

    }

    /**
     * Return the first node in order's schema
     *
     * @param string $schemaTag
     *
     * @return bool|mixed
     */
    public function getOrderFirst($schemaTag)
    {
        $first = $this->getNode('.first', $schemaTag, false);
        if ($first !== false) if (empty($first['id'])) $first = false;

        return $first;
    }

    /**
     * Set the first node in order's schema
     *
     * @param string $schemaTag
     * @param string $orderId
     *
     * @return mixed
     */
    private function setOrderFirst($schemaTag, $orderId)
    {
        return $this->putNode('.first', [
            'id' => $orderId,
            'last_update' => date("Y-m-d h:i:s")
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
    private function setOrderLast($schemaTag, $orderId)
    {
        return $this->putNode('.last', [
            'id' => $orderId,
            'last_update' => date("Y-m-d h:i:s")
        ], $schemaTag);
    }

    /**
     * Return the last node in order's schema
     *
     * @param string $schemaTag
     *
     * @return bool|mixed
     */
    public function getOrderLast($schemaTag)
    {
        $last = $this->getNode('.last', $schemaTag, false);
        if ($last !== false) if (empty($last['id'])) $last = false;

        return $last;
    }

    /**
     * Add order
     *
     * @param mixed $value
     * @param string $nodeId
     * @param string $tag
     * @param string $schema
     * @param string $schemaOrder
     *
     * @throws Exception
     *
     * @return boolean
     */
    public function addOrder($value, $nodeId, $tag = 'default', $schema = null, $schemaOrder = null)
    {
        if (is_null($schema)) $schema = $this->schema;
        if (is_null($schemaOrder)) $schemaOrder = $schema . "/.order";

        $schemaTag = "$schemaOrder/$tag";

        $this->addSchema($schemaTag);

        // wait for unlocked list
        return $this->waitAndDo(DIV_NODES_ROOT . "$schemaTag/.queue/.schema", function (divNodes $db, $params) {

            $schemaTag = $params['schemaTag'];
            $schema = $params['schema'];
            $nodeId = $params['nodeId'];
            $value = $params['value'];
            $newNode = false;
            $orderId = md5("$schema/$nodeId");
            $first = $db->getOrderFirst($schemaTag);
            $last = $db->getOrderLast($schemaTag);

            // check if no nodes
            if ($first === false) {
                // insert the first
                $db->setOrderFirst($schemaTag, $orderId);
                $db->setOrderLast($schemaTag, $orderId);

                $newNode = [
                    "schema" => $schema,
                    "id" => $nodeId,
                    "next" => false,
                    "previous" => false,
                    "value" => $value,
                    'last_update' => date("Y-m-d h:i:s")
                ];
            } else {

                $firstOrder = $db->getNode($first['id'], $schemaTag);
                $lastOrder = $db->getNode($last['id'], $schemaTag);
                $current = $first['id'];
                $currentOrder = $firstOrder;

                do {
                    if ($currentOrder['value'] > $value) {
                        if ($currentOrder['previous'] === false) // insert on top
                        {
                            $newNode = [
                                "schema" => $schema,
                                "id" => $nodeId,
                                "next" => $current,
                                "previous" => false,
                                "value" => $value,
                                'last_update' => date("Y-m-d h:i:s")
                            ];

                            $currentOrder['previous'] = $orderId;
                            $db->putNode($current, $currentOrder, $schemaTag);
                            $db->setOrderFirst($schemaTag, $orderId);
                            break;
                        }

                        // insert before
                        $newNode = [
                            "schema" => $schema,
                            "id" => $nodeId,
                            "next" => $current,
                            "previous" => $currentOrder['previous'],
                            "value" => $value,
                            'last_update' => date("Y-m-d h:i:s")
                        ];

                        $previous = $currentOrder['previous'];
                        $currentOrder['previous'] = $orderId;
                        $previousNode = $db->getNode($previous, $schemaTag);
                        $previousNode['next'] = $orderId;

                        $db->putNode($current, $currentOrder, $schemaTag);
                        $db->putNode($previous, $previousNode, $schemaTag);
                        break;
                    }

                    if ($currentOrder['next'] === false) break;

                    $current = $currentOrder['next'];
                    $currentOrder = $db->getNode($current, $schemaTag);

                } while ($currentOrder['next'] !== false);

                // insert on bottom
                if ($newNode === false) {
                    $lastOrder['next'] = $orderId;
                    $db->putNode($last['id'], $lastOrder, $schemaTag);

                    $newNode = [
                        "schema" => $schema,
                        "id" => $nodeId,
                        "next" => false,
                        "previous" => $last['id'],
                        "value" => $value,
                        'last_update' => date("Y-m-d h:i:s")
                    ];

                    $db->setOrderLast($schemaTag, $orderId);
                }
            }

            if ($newNode !== false) {
                $db->addNode($newNode, $orderId, $schemaTag);
                $db->addInverseIndex($nodeId, $schema, $orderId, $schemaTag);
                return true;
            }

            return false;
        }, [
            'schemaTag' => $schemaTag,
            'schema' => $schema,
            'schemaOrder' => $schemaOrder,
            'nodeId' => $nodeId,
            'value' => $value
        ]);
    }

    /**
     * For each order
     *
     * @param mixed $closure
     * @param string $tag
     * @param integer $offset
     * @param integer $limit
     * @param bool $fromFirst
     * @param array $otherData
     * @param string $schema
     * @param string $schemaOrder
     *
     * @return mixed
     */
    public function foreachOrder($closure, $tag = 'default', $offset = 0, $limit = -1, $fromFirst = true, &$otherData = [], $schema = null, $schemaOrder = null)
    {

        if (is_array($closure)) {
            $tag = isset($closure['tag']) ? $closure['tag'] : $tag;
            $offset = isset($closure['offset']) ? $closure['offset'] : $offset;
            $limit = isset($closure['limit']) ? $closure['limit'] : $limit;
            $fromFirst = isset($closure['fromFirst']) ? $closure['fromFirst'] : $fromFirst;
            $otherData = isset($closure['otherData']) ? $closure['otherData'] : $otherData;
            $schema = isset($closure['schema']) ? $closure['schema'] : $schema;
            $schemaOrder = isset($closure['schemaOrder']) ? $closure['schemaOrder'] : $schemaOrder;
            $closure = isset($closure['closure']) ? $closure['closure'] : function () {
            };
        }

        if (is_null($schema)) $schema = $this->schema;
        if (is_null($schemaOrder)) $schemaOrder = $schema . "/.order";

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
                if ($iterator < $offset) continue;

                $result = $closure($currentNode, $iterator, $otherData);

                if ($result == DIV_NODES_FOR_BREAK) break;

                $current = $fromFirst ? $currentNode['next'] : $currentNode['previous'];
                $currentNode = $current !== false ? $this->getNode($current, $schemaTag) : null;

            } while ($current !== false && ($iterator < $limit || $limit == -1));
        }

        return $otherData;
    }

    /**
     * Clear double slashes in ways
     *
     * @param $value
     *
     * @return mixed
     */
    static function clearDoubleSlashes($value)
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
    static function replaceRecursive($search, $replace, $source)
    {
        while (strpos($source, $search) !== false)
            $source = str_replace($search, $replace, $source);

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
    public function addInverseIndex($nodeId, $schema, $index, $wordSchema)
    {
        $node = $this->getNode("$nodeId.idx", $schema, [
            "indexes" => [],
            "last_update" => date("Y-m-d h:i:s")
        ]);

        $node['indexes'][$wordSchema] = $index;
        $node['last_update'] = date("Y-m-d h:i:s");
        $this->putNode("$nodeId.idx", $node, $schema);

        return $node;
    }


    /**
     * Wait for a turn in the queue and do something
     *
     * @param $queueFolder
     * @param $closure
     * @param $params
     * @param int $max_execution_time
     * @param string $extraFunctionWait
     * @param array $extraFunctionWaitParams
     *
     * @throws Exception
     *
     * @return mixed
     */
    public function waitAndDo($queueFolder, $closure, $params, $max_execution_time = 60, $extraFunctionWait = '', $extraFunctionWaitParams = [])
    {
        $threadId = $this->getThreadID();
        @mkdir($queueFolder, 0777, true);
        file_put_contents("$queueFolder/$threadId", $max_execution_time); // max execution time in seconds for this thread

        $i = 0;
        $last = null;
        $file = null;
        do {

            if (!is_null($extraFunctionWait) && !empty($extraFunctionWait))
                if ($extraFunctionWait($extraFunctionWaitParams) === false)
                    continue;

            // current thread
            $dir = opendir($queueFolder);
            $file = readdir($dir);
            while ($file == '.' || $file == '..') $file = @readdir($dir);
            closedir($dir);

            if ($file == false)
                throw new Exception("No access to the queue");

            // si al mismo tiempo otro proceso ya borro el item actual, pues ignorar y continuar
            $max = intval(@file_get_contents("$queueFolder/$file")); // obtengo el maximo tiempo del hilo actual

            if ($file === $threadId) break; // es mi turno, detengo el ciclo

            if ($last !== $file) $i = 0; // si otro hilo borro el item, renovar el conteo de max

            $last = $file;

            usleep(1000); //sleep 1/1000 seconds

            $i++;

            if ($i > $max * 100) {
                // ignorar si el archivo no existe
                // release the item and continue
                @unlink("$queueFolder/$file");
                $i = 0;
            }

        } while ($file !== false && $file !== $threadId);

        // Turn for me...Do something...

        $result = $closure($this, $params);

        // Done!

        // Destroy thread in the queue
        @unlink("$queueFolder/$threadId");

        return $result;
    }

    /**
     * Wait for exclusive access to node, and do a closure
     *
     * @param $nodeId
     * @param $closure
     * @param $params
     * @param int $max_execution_time
     * @param null $schema
     * @param string $queueSchema
     * @throws Exception
     *
     * @return mixed
     */
    public function waitForNodeAndDo($nodeId, $closure, $params, $max_execution_time = 60, $schema = null, $queueSchema = '.queue')
    {
        if (is_null($schema)) $schema = $this->schema;

        if (!file_exists(DIV_NODES_ROOT . "$schema/$queueSchema"))
            @mkdir(DIV_NODES_ROOT . "$schema/$queueSchema");

        if (!file_exists(DIV_NODES_ROOT . "$schema/$queueSchema/$nodeId")) {
            $queueFolder = date("Ymdhis") . uniqid("", true);
            file_put_contents(DIV_NODES_ROOT . "$schema/$queueSchema/$nodeId", $queueFolder);
            file_put_contents(DIV_NODES_ROOT . "$schema/$queueSchema/$queueFolder.idx", $nodeId);
            @mkdir(DIV_NODES_ROOT . "$schema/$queueSchema/$queueFolder", 0777, true);
        }

        $queueFolder = file_get_contents(DIV_NODES_ROOT . "$schema/$queueSchema/$nodeId");

        $result = $this->waitAndDo(DIV_NODES_ROOT . "$schema/$queueSchema/$queueFolder", $closure, $params, $max_execution_time, function ($params) {

            // search if exists any pending thread using a locked schema
            $dir = @opendir(DIV_NODES_ROOT . "{$params['schema']}/{$params['queueSchema']}/.schema");
            if ($dir === false) return true;

            $file = @readdir($dir);
            while ($file == '.' || $file == '..') $file = @readdir($dir);
            closedir($dir);

            if ($file === false) return true;
            if ($file === $params['threadId']) return true; // schema locked by it self
            return false;

        }, [
            "schema" => $schema,
            "queueSchema" => $queueSchema,
            "threadId" => $this->getThreadID()
        ]);

        // trash collector
        self::trash($this->getInstanceId(), function ($params) {

            $schema = $params['schema'];
            $queueSchema = $params['queueSchema'];
            $nodeId = $params['nodeId'];
            $queueFolder = @file_get_contents(DIV_NODES_ROOT . "$schema/$queueSchema/$nodeId");

            if ($queueFolder !== false && @rmdir(DIV_NODES_ROOT . "$schema/$queueSchema/$queueFolder")) {
                @unlink(DIV_NODES_ROOT . "$schema/$queueSchema/$nodeId");
                @unlink(DIV_NODES_ROOT . "$schema/$queueSchema/$queueFolder.idx");
            }

            @rmdir(DIV_NODES_ROOT . "$schema/$queueSchema");
        }, [
            'schema' => $schema,
            'queueSchema' => $queueSchema,
            'nodeId' => $nodeId
        ]);

        return $result;
    }

    /**
     * Save trash operation
     *
     * @param string $instanceId
     * @param closure $closure
     * @param array $params
     */
    private static function trash($instanceId, $closure, $params = [])
    {
        if (!isset(self::$__trash[$instanceId]))
            self::$__trash[$instanceId] = [];

        self::$__trash[$instanceId][] = [
            'f' => $closure,
            'p' => $params
        ];
    }

    /**
     * Execute trash operations
     *
     * @param string $instanceId
     */
    public static function emptyTrash($instanceId)
    {
        if (!isset(self::$__trash[$instanceId]))
            self::$__trash[$instanceId] = [];

        foreach (self::$__trash[$instanceId] as $t)
            $t['f']($t['p']);

        self::$__trash[$instanceId] = [];
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
