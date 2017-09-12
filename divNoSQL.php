<?php

/**
 * divNoSQL
 *
 * PHP NoSQL Database System
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
 * http://www.gnu.org/licenses/old-licenses/gpl-2.0.txt.
 *
 * @author Rafa Rodriguez <rafageist86@gmail.com>
 * @version 1.2
 * @link http://github.com/divengine/divNoSQL     
 */

/* CONSTANTS */
if (! defined("DIV_NOSQL_ROOT")) define("DIV_NOSQL_ROOT", "./");
if (! defined("DIV_NOSQL_LOG_FILE")) define("DIV_NOSQL_LOG_FILE", DIV_NOSQL_ROOT . "/divNoSQL.log");

define("DIV_NOSQL_ROLLBACK_TRANSACTION", "DIV_NOSQL_ROLLBACK_TRANSACTION");

/**
 * The class
 */
class divNoSQL
{

    var $schema = null;

    static $__log_mode = false;
    static $__log_file = DIV_NOSQL_LOG_FILE;
    static $__log_messages = array();

    /**
     * Constructor
     *
     * @param string $schema            
     * @return divNoSQL
     */
    public function __construct ($schema)
    {
        $this->setSchema($schema);
    }

    /**
     * Complete object/array properties
     *
     * @param mixed $obj            
     * @param mixed $prop            
     * @return mixed
     */
    final static function cop (&$source, $complement, $level = 0)
    {
        $null = null;
        
        if (is_null($source)) return $complement;
        
        if (is_null($complement)) return $source;
        
        if (is_scalar($source) && is_scalar($complement)) return $complement;
        
        if (is_scalar($complement) || is_scalar($source)) return $source;
        
        if ($level < 100) { // prevent infinite loop
            if (is_object($complement)) $complement = get_object_vars($complement);
            
            foreach ($complement as $key => $value) {
                if (is_object($source)) {
                    if (isset($source->$key))
                        $source->$key = self::cop($source->$key, $value, $level + 1);
                    else
                        $source->$key = self::cop($null, $value, $level + 1);
                }
                if (is_array($source)) {
                    if (isset($source[$key]))
                        $source[$key] = self::cop($source[$key], $value, $level + 1);
                    else
                        $source[$key] = self::cop($null, $value, $level + 1);
                }
            }
        }
        return $source;
    }

    /**
     * Add schema
     *
     * @param string $schema            
     */
    public function addSchema ($schema)
    {
        $arr = explode("/", $schema);
        $path = DIV_NOSQL_ROOT;
        foreach ($arr as $d) {
            $path .= "$d/";
            if (! file_exists($path)) mkdir($path);
        }
    }

    /**
     * Set the schema of work
     *
     * @param string $schema            
     */
    public function setSchema ($schema)
    {
        $this->addSchema($schema);
        $this->schema = $schema;
    }

    /**
     * Get recursive list of schemas
     *
     * @param $from
     * @return array
     */
    public function getSchemas($from)
    {
        $schemas = [];

        if ($this->existsSchema($from))
        {
            $schemas[] = $from;

            $stack = [$from => $from];

            while (count($stack) > 0) // avoid recursive calls!!
            {
                $from = array_shift($stack);

                $dir = scandir(DIV_NOSQL_ROOT . $from);

                foreach ($dir as $entry)
                {
                    $fullSchema = str_replace("//", "/", "$from/$entry");

                    if ($entry != '.' && $entry != '..' && ! is_file(DIV_NOSQL_ROOT . $fullSchema))
                    {
                        $stack[$fullSchema] = $fullSchema;
                        $schemas[] = $fullSchema;
                    }
                }
            }
        }

        return $schemas;
    }

    /**
     * Rename a schema
     *
     * @param string $schema            
     * @param string $newname
     * @return boolean
     */
    public function renameSchema ($newname, $schema)
    {
        if (is_null($schema)) $schema = $this->schema;
        
        if (! $this->existsSchema($schema)) return false;
        
        $restore = $schema === $this->schema;
        
        rename(DIV_NOSQL_ROOT . $schema, DIV_NOSQL_ROOT . $newname);
        
        if ($restore) $this->schema = $newname;
        
        return true;
    }

    /**
     * Remove a schema
     *
     * @param string $schema            
     * @return boolean
     */
    public function delSchema ($schema)
    {
        if (file_exists(DIV_NOSQL_ROOT . $schema)) {
            if (! is_dir(DIV_NOSQL_ROOT . $schema)) return false;
            $dir = scandir(DIV_NOSQL_ROOT . $schema);
            foreach ($dir as $entry) {
                if ($entry != "." && $entry != "..") {
                    if (is_dir(DIV_NOSQL_ROOT . $schema . "/$entry")) {
                        $this->delSchema($schema . "/$entry");
                    } else {
                        if ($entry != ".locks" && $entry != ".references") $this->delNode($entry, $schema);
                    }
                }
            }
            
            if (file_exists(DIV_NOSQL_ROOT . $schema . "/.locks")) unlink(DIV_NOSQL_ROOT . $schema . "/.locks");
            
            // Remove orphan references
            $references = $this->getReferences($schema);
            
            foreach ($references as $rel) {
                
                if ($rel['foreign_schema'] == $schema)
                    $sch = $rel['schema'];
                else
                    $sch = $rel['foreign_schema'];
                    
                    // If the schema of reference is a subschema of this schema
                if ($schema == substr($sch, 0, strlen($schema))) continue;
                
                $relats = $this->getReferences($sch);
                $newreferences = array();
                foreach ($relats as $re) {
                    if ($re['schema'] != $schema && $re['foreign_schema'] != $schema) $newreferences[] = $re;
                }
                file_put_contents(DIV_NOSQL_ROOT . $sch . "/.references", serialize($newreferences));
            }
            
            unlink(DIV_NOSQL_ROOT . $schema . "/.references");
            rmdir(DIV_NOSQL_ROOT . $schema);
            return true;
        }
        return false;
    }

    /**
     * Insert a node in schema
     *
     * @param mixed $node            
     * @param scalar $id            
     * @param string $schema            
     * @return scalar
     */
    public function addNode ($node, $id = null, $schema = null)
    {
        if (is_null($schema)) $schema = $this->schema;
        if (is_null($id)) $id = date("Ymdhis") . uniqid();
        
        if ($id == ".references" || $id == ".locks") {
            self::log("Invalid ID '$id' for node");
            return false;
        }
        
        $node = $this->triggerBeforeAdd($node, $id, $schema);
        
        if ($node == false) return false;
        
        $data = serialize($node);
        
        file_put_contents(DIV_NOSQL_ROOT . $schema . "/$id", $data);
        
        $this->lockNode($id, $schema);
        
        $r = $this->triggerAfterAdd($node, $id, $schema);
        
        if ($r === DIV_NOSQL_ROLLBACK_TRANSACTION) {
            unlink(DIV_NOSQL_ROOT . $schema . "/$id");
            $this->unlockNode($id, $schema);
            return DIV_NOSQL_ROLLBACK_TRANSACTION;
        }
        
        $this->unlockNode($id, $schema);
        
        return $id;
    }

    public function triggerBeforeAdd ($node, $id, $schema)
    {
        return $node;
    }

    public function triggerAfterAdd ($node, $id, $schema)
    {
        return $node;
    }

    /**
     * Return a node
     *
     * @param scalar $id            
     * @param string $schema            
     * @return mixed
     */
    public function getNode ($id, $schema = null, $default = null)
    {
        if (is_null($schema)) $schema = $this->schema;
        if (file_exists(DIV_NOSQL_ROOT . $schema . "/$id") && is_file(DIV_NOSQL_ROOT . $schema . "/$id"))
            $data = file_get_contents(DIV_NOSQL_ROOT . $schema . "/$id");
        else
            return $default;
        $sec = 0;
        while ($this->isLockNode($id, $schema) || $sec > 999999) {
            $sec ++;
        }
        $this->lockNode($id, $schema);
        $node = unserialize($data);
        $this->unlockNode($id, $schema);
        return $node;
    }

    /**
     * Return a list of node's id
     *
     * @param string $schema            
     * @return array
     */
    public function getNodesID ($schema = null)
    {
        if (is_null($schema)) $schema = $this->schema;
        
        if (! $this->existsSchema($schema)) return false;
        
        $list = array();
        $dir = scandir(DIV_NOSQL_ROOT . $schema);
        foreach ($dir as $entry) {
            if (! is_dir(DIV_NOSQL_ROOT . $schema . "/$entry")) {
                if ($entry != ".references" && $entry != ".locks") $list[] = $entry;
            }
        }
        
        return $list;
    }

    /**
     * Return a list of nodes
     *
     * @param array $params            
     * @param string $schema            
     * @return array
     */
    public function getNodes ($params = array(), $schema = null, $onlyIds = false)
    {
        if (is_null($schema)) $schema = $this->schema;
        
        if (! $this->existsSchema($schema)) return false;
        
        $dp = array(
                "where" => "true",
                "offset" => 0,
                "limit" => - 1,
                "fields" => "*",
                "order" => null,
                "order_asc" => true
        );
        
        $params = self::cop($dp, $params);
        $ids = $this->getNodesID($schema);

        // get result
        $newIds = [];
        foreach ($ids as $id) {

            $node = $this->getNode($id, $schema);

            $vars = [];
            if (is_object($node)) $vars = get_object_vars($node);
            elseif (is_array($node)) $vars = $node;
            elseif (is_scalar($node)) $vars = ['value' => $node];
            $w = $params['where'];

            foreach ($vars as $key => $value) {
                $w = str_replace('{' . $key . '}', '$vars["' . $key . '"]', $w);
            }

            $w = str_replace('{id}', '$id', $w);

            $r = false;
            eval('$r = ' . $w . ';');

            if ($r === true) {
                /*if (is_object($node) || is_array($node)) {
                    $fields = explode(",", $params['fields']);
                    foreach ($fields as $key => $value) $fields[$value] = true;
                    foreach ($vars as $key => $value)
                        if (! isset($fields[$key]) && ! isset($fields['*'])) {
                            if (is_object($node)) unset($node->$key);
                            elseif (is_array($node)) unset($node[$key]);
                        }
                }*/

                $newIds[] = $id;
            }
        }

        // sort results
        $order = $params['order'];

        if ($order !== false && ! is_null($order))
        {
            $sorted = array();
            foreach ($newIds as $id)
            {
                $node = $this->getNode($id, $schema);
                $sorted[$id] = $node;
                if (is_object($node) && isset($node->$order)) $sorted[$id] = $node->$order;
                if (is_array($node) && isset($node[$order])) $sorted[$id] = $node[$order];
            }

            if (asort($sorted))
            {
                if ($params['order_asc'] === false) $sorted = array_reverse($sorted);
                $newIds = $sorted;
            }
        }

        // limit result
        $list = [];
        $i = 0;
        $c = 0;
        foreach ($newIds as $id)
        {
            if ($i >= $params['offset'])
                if ($c < $params['limit'] || $params['limit'] == - 1)
                {
                    if ( ! $onlyIds) $list[$id] = $this->getNode($id, $schema);
                    else $list[] = $id;
                    $c++;
                }
            $i++;
        }

        return $list;
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
     * @return array
     */
    public function getRecursiveNodes($schema = "/", $paramsBySchema = [], $paramsDefault = [], $offset = 0, $limit = -1, $onlyIds = false)
    {
        $schemas = [$schema];
        $schemas = array_merge($schemas, $this->getSchemas($schema));

        $nodes = [];
        foreach ($schemas as $schema)
        {
            $params = $paramsDefault;
            if (isset($paramsBySchema[$schema]))
            {
                $params = $paramsBySchema[$schema];
                $params = array_merge($paramsDefault, $params);
            }

            $list = $this->getNodes($params, $schema, true);

            if ($list !== false)
                $nodes[$schema] = $list;
        }

        // limit result
        $list = [];
        $i = 0;
        $c = 0;
        foreach ($nodes as $schema => $ids)
        {
            foreach($ids as $id)
            {
                if ($i >= $offset)
                {
                    if ($c < $limit || $limit == - 1)
                    {
                        if ( ! isset($list[$schema])) $list[$schema] = [];
                        if ( ! $onlyIds) $list[$schema][$id] = $this->getNode($id, $schema);
                        else $list[$schema][$id] = $id;
                        $c++;
                    }
                }
            }
            $i++;
        }

        return $list;
    }

    /**
     * Return the count of nodes
     *
     * @param array $params            
     * @param string $schema            
     * @return integer
     */
    public function getCount ($params = array(), $schema = null)
    {
        if (is_null($schema)) $schema = $this->schema;
        
        if (! $this->existsSchema($schema)) return false;
        
        $dp = array(
                "where" => "true"
        );
        $params = self::cop($dp, $params);
        
        $ids = $this->getNodesID($schema);
        $list = array();
        
        $c = 0;
        foreach ($ids as $id) {
            $node = $this->getNode($id, $schema);
            
            if (is_object($node))
                $vars = get_object_vars($node);
            elseif (is_array($node))
                $vars = $node;
            elseif (is_scalar($node))
                $vars = array(
                        'value' => $node
                );
            
            $w = $params['where'];
            foreach ($vars as $key => $value) {
                $w = str_replace('{' . $key . '}', '$vars["' . $key . '"]', $w);
            }
            $w = str_replace('{id}', $id, $w);
            
            $r = false;
            eval('$r = ' . $w . ';');
            if ($r === true) $c ++;
        }
        return $c;
    }

    /**
     * Remove one node
     *
     * @param scalar $id            
     * @param string $schema            
     * @return boolean
     */
    public function delNode ($id, $schema = null)
    {
        if (is_null($schema)) $schema = $this->schema;
        
        if (! $this->existsSchema($schema)) return false;
        
        if (file_exists(DIV_NOSQL_ROOT . $schema . "/$id")) {
            $sec = 0;
            while ($this->isLockNode($id, $schema) || $sec > 999999) {
                $sec ++;
            }
            $this->lockNode($id, $schema);
            
            $r = $this->triggerBeforeDel($id, $schema);
            if ($r === DIV_NOSQL_ROLLBACK_TRANSACTION) {
                $this->unlockNode($id, $schema);
                return DIV_NOSQL_ROLLBACK_TRANSACTION;
            }
            
            $restore = array();
            // Delete cascade
            $references = $this->getReferences($schema);
            foreach ($references as $rel) {
                if ($rel['foreign_schema'] == $schema) {
                    if (! $this->existsSchema($rel['schema'])) continue;
                    $ids = $this->getNodesID($rel['schema']);
                    foreach ($ids as $fid) {
                        $node = $this->getNode($fid, $rel['schema']);
                        
                        $restore[] = array(
                                "node" => $node,
                                "id" => $fid,
                                "schema" => $rel['schema']
                        );
                        
                        $procede = false;
                        
                        if (is_array($node)) {
                            if (isset($node[$rel['property']])) {
                                if ($node[$rel['property']] == $id) {
                                    if ($rel['delete_cascade'] == true)
                                        $procede = true;
                                    else
                                        $this->setNode($fid, array(
                                                $rel['property'] => null
                                        ), $rel['schema']);
                                }
                            }
                        } elseif (is_object($node)) {
                            if (isset($node->$rel['property'])) {
                                if ($node->$rel['property'] == $id) {
                                    if ($rel['delete_cascade'] == true)
                                        $procede = true;
                                    else
                                        $this->setNode($fid, array(
                                                $rel['property'] => null
                                        ), $rel['schema']);
                                }
                            }
                        }
                        
                        if ($procede) {
                            $r = $this->delNode($fid, $rel['schema']);
                            if ($r === DIV_NOSQL_ROLLBACK_TRANSACTION) {
                                return DIV_NOSQL_ROLLBACK_TRANSACTION;
                            }
                        }
                    }
                }
            }
            
            $r = $this->triggerAfterDel($id, $schema);
            
            if ($r === DIV_NOSQL_ROLLBACK_TRANSACTION) {
                foreach ($restore as $rest) {
                    if ($this->existsNode($rest['id'], $rest['schema'])) {
                        $this->setNode($rest['id'], $rest['node'], $rest['schema']);
                    } else {
                        $this->addNode($rest['node'], $rest['id'], $rest['schema']);
                    }
                }
                return DIV_NOSQL_ROLLBACK_TRANSACTION;
            }
            
            // Delete the node
            unlink($schema . "/$id");
            $this->unlockNode($id, $schema);
            return true;
        }
        return false;
    }

    public function triggerBeforeDel ($id, $schema)
    {
        return '';
    }

    public function triggerAfterDel ($id, $schema)
    {
        return '';
    }

    /**
     * Remove some nodes
     *
     * @param array $params            
     * @param string $schema            
     */
    public function delNodes ($params = array(), $schema = null)
    {
        if (is_null($schema)) $schema = $this->schema;
        
        if (! $this->existsSchema($schema)) return false;
        
        $dp = array(
                "where" => "true",
                "offset" => 0,
                "limit" => - 1
        );
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
    }

    /**
     * Update data of a node
     *
     * @param scalar $id            
     * @param mixed $data            
     * @param string $schema            
     */
    public function setNode ($id, $data, $schema = null)
    {
        if (is_null($schema)) $schema = $this->schema;
        
        if (! $this->existsSchema($schema)) return false;
        
        $node = $this->getNode($id, $schema);
        
        $r = $this->triggerBeforeSet($id, $node, $data);
        if ($r === DIV_NOSQL_ROLLBACK_TRANSACTION) {
            return DIV_NOSQL_ROLLBACK_TRANSACTION;
        }
        
        $sec = 0;
        while ($this->isLockNode($id, $schema) || $sec > 999999) {
            $sec ++;
        }
        $this->lockNode($id, $schema);
        
        $old = $node;
        $node = self::cop($node, $data);
        
        file_put_contents(DIV_NOSQL_ROOT . $schema . "/$id", serialize($node));
        
        $r = $this->triggerAfterSet($id, $old, $node, $data);
        
        if ($r === DIV_NOSQL_ROLLBACK_TRANSACTION) {
            file_put_contents($schema . "/$id", serialize($old));
        }
        
        $this->unlockNode($id, $schema);
    }

    public function triggerAfterSet ($id, &$old, $new)
    {
        return true;
    }

    public function triggerBeforeSet ($id, &$node, &$data)
    {
        return true;
    }

    /**
     * Set id of Node
     *
     * @param scalar $id_old            
     * @param scalar $id_new            
     * @param string $schema            
     */
    public function setNodeID ($id_old, $id_new, $schema = null)
    {
        if (is_null($schema)) $schema = $this->schema;
        
        if (! $this->existsSchema($schema)) return false;
        
        $sec = 0;
        while ($this->isLockNode($id_old, $schema) || $sec > 999999) {
            $sec ++;
        }
        
        $this->lockNode($id_old, $schema);
        $this->lockNode($id_new, $schema);
        
        // Update cascade
        $references = $this->getReferences($schema);
        foreach ($references as $rel) {
            if ($rel['foreign_schema'] == $schema && $rel['update_cascade'] == true) {
                $ids = $this->getNodesID($rel['schema']);
                foreach ($ids as $fid) {
                    $node = $this->getNode($fid, $rel['schema']);
                    
                    $procede = false;
                    
                    if (is_array($node)) {
                        if (isset($node[$rel['property']])) {
                            if ($node[$rel['property']] == $id_old) $procede = true;
                        }
                    } elseif (is_object($node)) {
                        if (isset($node->$rel['property'])) {
                            if ($node->$rel['property'] == $id_old) $procede = true;
                        }
                    }
                    
                    if ($procede) $this->setNode($fid, array(
                            $rel['property'] => $id_new
                    ), $rel['schema']);
                }
            }
        }
        
        rename(DIV_NOSQL_ROOT . $schema . "/$id_old", DIV_NOSQL_ROOT . $schema . "/$id_new");
        
        $this->unlockNode($id_old, $schema);
        $this->unlockNode($id_new, $schema);
    }

    /**
     * Know if schema exists
     *
     * @param string $schema            
     * @return boolean
     */
    public function existsSchema ($schema = null)
    {
        if (is_null($schema)) $schema = $this->schema;
        
        if (file_exists(DIV_NOSQL_ROOT . $schema)) {
            if (is_dir(DIV_NOSQL_ROOT . $schema)) return true;
        }
        
        self::log("Schema $schema not exists");
        
        return false;
    }

    /**
     * Know if node exists
     *
     * @param string $id            
     * @param string $schema            
     * @return boolean
     */
    public function existsNode ($id, $schema = null)
    {
        if (is_null($schema)) $schema = $this->schema;
        
        if (file_exists(DIV_NOSQL_ROOT . $schema . "/$id")) {
            if (! is_dir(DIV_NOSQL_ROOT . $schema . "/$id")) return true;
        }
        
        return false;
    }

    /**
     * Return a list of schema's references
     *
     * @param string $schema            
     * @return array
     */
    public function getReferences ($schema = null)
    {
        if (is_null($schema)) $schema = $this->schema;
        
        if (! $this->existsSchema($schema)) return false;
        
        $path = DIV_NOSQL_ROOT . $schema . "/.references";
        if (! file_exists($path)) {
            file_put_contents($path, serialize(array()));
        }
        $data = file_get_contents($path);
        return unserialize($data);
    }

    /**
     * Get list of lock nodes
     *
     * @param string $schema            
     * @return array
     */
    private function getLocks ($schema = null)
    {
        if (is_null($schema)) $schema = $this->schema;
        
        if (! $this->existsSchema($schema)) return false;
        
        $path = DIV_NOSQL_ROOT . $schema . "/.locks";
        if (! file_exists($path)) {
            file_put_contents($path, serialize(array()));
        }
        $data = file_get_contents($path);
        return unserialize($data);
    }

    /**
     * Lock a node
     *
     * @param scalar $id            
     * @param string $schema            
     * @return boolean
     */
    private function lockNode ($id, $schema = null)
    {
        if (is_null($schema)) $schema = $this->schema;
        if (! $this->existsSchema($schema)) return false;
        
        if (! file_exists(DIV_NOSQL_ROOT . $schema . "/" . $id)) return false;
        
        $blocked = $this->getLocks($schema);
        $blocked[$id] = true;
        $path = DIV_NOSQL_ROOT . $schema . "/" . ".locks";
        
        file_put_contents($path, serialize($blocked));
        
        return true;
    }

    /**
     * Unlock a node
     *
     * @param scalar $id            
     * @param string $schema            
     * @return boolean
     */
    private function unlockNode ($id, $schema = null)
    {
        if (is_null($schema)) $schema = $this->schema;
        if (! $this->existsSchema($schema)) return false;
        
        $blocked = $this->getLocks($schema);
        
        if (isset($blocked[$id])) unset($blocked[$id]);
        
        $path = DIV_NOSQL_ROOT . $schema . "/.locks";
        $nlocks = array();
        
        foreach ($blocked as $lock)
            if (file_exists(DIV_NOSQL_ROOT . $schema . "/$lock") && $id != $lock) $nlocks[] = $lock;
        
        file_put_contents($path, serialize($nlocks));
        return true;
    }

    /**
     * Know if node are lock
     *
     * @param scalar $id            
     * @param string $schema            
     * @return boolean
     */
    public function isLockNode ($id, $schema = null)
    {
        if (is_null($schema)) $schema = $this->schema;
        
        if (! $this->existsSchema($schema)) return false;
        
        if (! file_exists(DIV_NOSQL_ROOT . $schema . "/" . $id)) return false;
        
        $blocked = $this->getLocks($schema);
        return isset($blocked[$id]);
    }

    /**
     * Add new reference for schema
     *
     * @param array $params            
     * @return boolean
     */
    public function addReference ($params = array())
    {
        $dp = array(
                "schema" => $this->schema,
                "foreign_schema" => $this->schema,
                "update_cascade" => true,
                "delete_cascade" => true
        );
        $params = self::cop($dp, $params);
        
        if (! isset($params['property'])) return false;
        
        $schema = $params['schema'];
        $foreign_schema = $params['foreign_schema'];
        
        if (! $this->existsSchema($schema)) return false;
        if (! $this->existsSchema($foreign_schema)) return false;
        
        $references = $this->getReferences($schema);
        $freferences = $this->getReferences($foreign_schema);
        
        foreach ($references as $rel)
            if (serialize($rel) == serialize($params)) return true;
        
        $references[] = $params;
        $freferences[] = $params;
        
        file_put_contents(DIV_NOSQL_ROOT . $schema . "/.references", serialize($references));
        file_put_contents(DIV_NOSQL_ROOT . $foreign_schema . "/.references", serialize($freferences));
        
        return true;
    }

    /**
     * Delete a reference
     *
     * @param array $params            
     * @return boolean
     */
    public function delReference ($params = array())
    {
        $dp = array(
                "schema" => $this->schema,
                "foreign_schema" => $this->schema,
                "update_cascade" => true,
                "delete_cascade" => true
        );
        
        $params = self::cop($dp, $params);
        
        if (! isset($params['property'])) return false;
        
        $schema = $params['schema'];
        $foreign_schema = $params['foreign_schema'];
        
        $references = $this->getReferences($schema);
        $newreferences = array();
        foreach ($references as $rel) {
            if ($rel['schema'] == $params['schema'] && $rel['foreign_schema'] == $params['foreign_schema'] && $rel['property'] == $params['property']) continue;
            $newreferences[] = $rel;
        }
        
        file_put_contents(DIV_NOSQL_ROOT . $schema . "/.references", serialize($newreferences));
        
        $references = $this->getReferences($foreign_schema);
        $newreferences = array();
        foreach ($references as $rel) {
            if ($rel['schema'] == $params['schema'] && $rel['foreign_schema'] == $params['foreign_schema'] && $rel['property'] == $params['property']) continue;
            
            $newreferences[] = $rel;
        }
        
        file_put_contents(DIV_NOSQL_ROOT . $foreign_schema . "/.references", serialize($newreferences));
        
        return true;
    }

    /**
     * Log messages
     *
     * @param string $message            
     * @param string $level            
     * @param string $file            
     */
    static function log ($message, $level = "INFO", $file = DIV_NOSQL_LOG_FILE)
    {
        if (self::$__log_mode) {
            $message = date("Y-m-d h:i:s") . "[$level] $message \n";
            echo $message;
            self::$__log_messages[] = $message;
            
            $f = fopen(self::$__log_file, 'a');
            fputs($f, $message);
            fclose($f);
        }
    }
}
