<?php

/**
 * divNodes
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
 * http://www.gnu.org/licenses/old-licenses/gpl-2.0.txt.
 *
 * @author  Rafa Rodriguez [@rafageist] <rafageist@hotmail.com>
 * @version 1.3
 * @link    http://github.com/divengine/divNodes
 */

/* CONSTANTS */
if( ! defined("DIV_NODES_ROOT")) define("DIV_NODES_ROOT", "./");
if( ! defined("DIV_NODES_LOG_FILE")) define("DIV_NODES_LOG_FILE", DIV_NODES_ROOT . "/divNodes.log");


define("DIV_NODES_FOR_BREAK", "DIV_NODES_FOR_BREAK");
define("DIV_NODES_FOR_CONTINUE_SAVING", "DIV_NODES_FOR_CONTINUE_SAVING");
define("DIV_NODES_FOR_CONTINUE_DISCARDING", "DIV_NODES_FOR_CONTINUE_DISCARDING");
define("DIV_NODES_ROLLBACK_TRANSACTION", "DIV_NODES_ROLLBACK_TRANSACTION");

/**
 * The class
 */
class divNodes
{

	static $__log_mode = false;
	static $__log_file = DIV_NODES_LOG_FILE;
	static $__log_messages = [];
	var $schema = null;

	/**
	 * Constructor
	 *
	 * @param string $schema
	 *
	 * @return divNodes
	 */
	public function __construct($schema)
	{
		$this->setSchema($schema);
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
		return $id == '.references' || $id == '.index' || $id == '.stats';
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
		$arr  = explode("/", $schema);
		$path = DIV_NODES_ROOT;
		foreach($arr as $d)
		{
			$path .= "$d/";
			if( ! file_exists($path))
			{
				mkdir($path);
			}
		}
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
		if(is_null($schema))
		{
			$schema = $this->schema;
		}

		if( ! $this->existsSchema($schema))
		{
			return false;
		}

		$restore = $schema === $this->schema;

		rename(DIV_NODES_ROOT . $schema, DIV_NODES_ROOT . $new_name);

		if($restore)
		{
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
		if(is_null($schema))
		{
			$schema = $this->schema;
		}

		if(file_exists(DIV_NODES_ROOT . $schema))
		{
			if(is_dir(DIV_NODES_ROOT . $schema))
			{
				return true;
			}
		}

		self::log("Schema $schema not exists");

		return false;
	}

	/**
	 * Log messages
	 *
	 * @param string $message
	 * @param string $level
	 * @param string $file
	 */
	static function log($message, $level = "INFO", $file = DIV_NODES_LOG_FILE)
	{
		if(self::$__log_mode)
		{
			$message = date("Y-m-d h:i:s") . "[$level] $message \n";
			echo $message;
			self::$__log_messages[] = $message;

			$f = fopen(self::$__log_file, 'a');
			fputs($f, $message);
			fclose($f);
		}
	}

	/**
	 * Remove a schema
	 *
	 * @param string $schema
	 *
	 * @return boolean
	 */
	public function delSchema($schema)
	{
		if(file_exists(DIV_NODES_ROOT . $schema))
		{
			if( ! is_dir(DIV_NODES_ROOT . $schema))
			{
				return false;
			}
			$dir = scandir(DIV_NODES_ROOT . $schema);
			foreach($dir as $entry)
			{
				if($entry != "." && $entry != "..")
				{
					if(is_dir(DIV_NODES_ROOT . $schema . "/$entry"))
					{
						$this->delSchema($schema . "/$entry");
					}
					else
					{
						if( ! $this->isReservedId($entry))
						{
							$this->delNode($entry, $schema);
						}
					}
				}
			}

			if(file_exists(DIV_NODES_ROOT . $schema . "/.locks")) unlink(DIV_NODES_ROOT . $schema . "/.locks");

			// Remove orphan references
			$references = $this->getReferences($schema);

			foreach($references as $rel)
			{

				if($rel['foreign_schema'] == $schema)
				{
					$sch = $rel['schema'];
				}
				else
				{
					$sch = $rel['foreign_schema'];
				}

				// If the schema of reference is a subschema of this schema
				if($schema == substr($sch, 0, strlen($schema)))
				{
					continue;
				}

				$relats         = $this->getReferences($sch);
				$new_references = [];
				foreach($relats as $re)
				{
					if($re['schema'] != $schema && $re['foreign_schema'] != $schema)
					{
						$new_references[] = $re;
					}
				}
				file_put_contents(DIV_NODES_ROOT . $sch . "/.references", serialize($new_references));
			}

			unlink(DIV_NODES_ROOT . $schema . "/.references");
			rmdir(DIV_NODES_ROOT . $schema);

			return true;
		}

		return false;
	}

	/**
	 * Remove one node
	 *
	 * @param scalar $id
	 * @param string $schema
	 *
	 * @return boolean
	 */
	public function delNode($id, $schema = null)
	{
		if(is_null($schema))
		{
			$schema = $this->schema;
		}

		if( ! $this->existsSchema($schema))
		{
			return false;
		}

		if(file_exists(DIV_NODES_ROOT . $schema . "/$id"))
		{
			$sec = 0;
			while($this->isLockNode($id, $schema) || $sec > 999999)
			{
				$sec ++;
			}
			$this->lockNode($id, $schema);

			$r = $this->triggerBeforeDel($id, $schema);
			if($r === DIV_NODES_ROLLBACK_TRANSACTION)
			{
				$this->unlockNode($id, $schema);

				return DIV_NODES_ROLLBACK_TRANSACTION;
			}

			$restore = [];
			// Delete cascade
			$references = $this->getReferences($schema);
			foreach($references as $rel)
			{
				if($rel['foreign_schema'] == $schema)
				{
					if( ! $this->existsSchema($rel['schema']))
					{
						continue;
					}
					$ids = $this->getNodesID($rel['schema']);
					foreach($ids as $fid)
					{
						$node = $this->getNode($fid, $rel['schema']);

						$restore[] = [
							"node" => $node,
							"id" => $fid,
							"schema" => $rel['schema']
						];

						$delete_node = false;

						if(is_array($node))
						{
							if(isset($node[ $rel['property'] ]))
							{
								if($node[ $rel['property'] ] == $id)
								{
									if($rel['delete_cascade'] == true)
									{
										$delete_node = true;
									}
									else
									{
										$this->setNode($fid, [
											$rel['property'] => null
										], $rel['schema']);
									}
								}
							}
						}
						elseif(is_object($node))
						{
							if(isset($node->$rel['property']))
							{
								if($node->$rel['property'] == $id)
								{
									if($rel['delete_cascade'] == true)
									{
										$delete_node = true;
									}
									else
									{
										$this->setNode($fid, [
											$rel['property'] => null
										], $rel['schema']);
									}
								}
							}
						}

						if($delete_node)
						{
							$r = $this->delNode($fid, $rel['schema']);
							if($r === DIV_NODES_ROLLBACK_TRANSACTION)
							{
								return DIV_NODES_ROLLBACK_TRANSACTION;
							}
						}
					}
				}
			}

			$r = $this->triggerAfterDel($id, $schema);

			if($r === DIV_NODES_ROLLBACK_TRANSACTION)
			{
				foreach($restore as $rest)
				{
					if($this->existsNode($rest['id'], $rest['schema']))
					{
						$this->setNode($rest['id'], $rest['node'], $rest['schema']);
					}
					else
					{
						$this->addNode($rest['node'], $rest['id'], $rest['schema']);
					}
				}

				return DIV_NODES_ROLLBACK_TRANSACTION;
			}

			// Delete the node
			unlink($schema . "/$id");
			$this->unlockNode($id, $schema);

			return true;
		}

		return false;
	}

	/**
	 * Know if node are lock
	 *
	 * @param mixed  $id
	 * @param string $schema
	 *
	 * @return boolean
	 */
	public function isLockNode($id, $schema = null)
	{
		return file_exists(DIV_NODES_ROOT . $schema . "/" . $id . ".lock");
	}


	/**
	 * Lock a node
	 *
	 * @param mixed  $id
	 * @param string $schema
	 *
	 * @return boolean
	 */
	private function lockNode($id, $schema = null)
	{
		if(is_null($schema)) $schema = $this->schema;
		$r = @file_put_contents(DIV_NODES_ROOT . $schema . "/" . $id . ".lock", '');

		return $r !== false;
	}

	public function triggerBeforeDel($id, $schema)
	{
		return '';
	}

	/**
	 * Unlock a node
	 *
	 * @param scalar $id
	 * @param string $schema
	 *
	 * @return boolean
	 */
	private function unlockNode($id, $schema = null)
	{
		if(is_null($schema)) $schema = $this->schema;

		return @unlink(DIV_NODES_ROOT . $schema . '/' . $id . '.lock');
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
		if(is_null($schema)) $schema = $this->schema;

		if( ! $this->existsSchema($schema))
		{
			return false;
		}

		$path = DIV_NODES_ROOT . $schema . "/.references";
		if( ! file_exists($path))
		{
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
	public function getNodesID($schema = null)
	{
		if(is_null($schema))
		{
			$schema = $this->schema;
		}

		if( ! $this->existsSchema($schema))
		{
			return false;
		}

		$list = [];
		$dir  = scandir(DIV_NODES_ROOT . $schema);

		foreach($dir as $entry)
		{
			$full_path = DIV_NODES_ROOT . $schema . "/$entry";
			if( ! is_dir($full_path))
			{
				if( ! $this->isReservedId($entry))
				{
					if(pathinfo($full_path, PATHINFO_EXTENSION) == "idx" && file_exists(substr($full_path, 0, strlen($full_path) - 4))) continue;
					$list[] = $entry;
				}
			}
		}

		return $list;
	}

	/**
	 * Return a node
	 *
	 * @param mixed  $id
	 * @param string $schema
	 * @param mixed  $default
	 *
	 * @return mixed
	 */
	public function getNode($id, $schema = null, $default = null)
	{
		if(is_null($schema)) $schema = $this->schema;

		// read pure data
		$data = @file_get_contents(DIV_NODES_ROOT . $schema . "/$id");
		if($data === false) return $default;

		// wait for unlocked
		$sec = 0;
		while($this->isLockNode($id, $schema) || $sec > 999999) $sec ++;

		// lock and load
		$this->lockNode($id, $schema);
		$node = unserialize($data);
		$this->unlockNode($id, $schema);

		return $node;
	}

	/**
	 * Update data of a node
	 *
	 * @param mixed  $id
	 * @param mixed  $data
	 * @param string $schema
	 *
	 * @return mixed
	 */
	public function setNode($id, $data, $schema = null)
	{
		if(is_null($schema))
		{
			$schema = $this->schema;
		}

		if( ! $this->existsSchema($schema))
		{
			return false;
		}

		$node = $this->getNode($id, $schema);

		$r = $this->triggerBeforeSet($id, $node, $data);
		if($r === DIV_NODES_ROLLBACK_TRANSACTION)
		{
			return DIV_NODES_ROLLBACK_TRANSACTION;
		}

		$sec = 0;
		while($this->isLockNode($id, $schema) || $sec > 999999)
		{
			$sec ++;
		}
		$this->lockNode($id, $schema);

		$old  = $node;
		$node = self::cop($node, $data);

		file_put_contents(DIV_NODES_ROOT . $schema . "/$id", serialize($node));

		$r = $this->triggerAfterSet($id, $old, $node, $data);

		if($r === DIV_NODES_ROLLBACK_TRANSACTION)
		{
			file_put_contents($schema . "/$id", serialize($old));
		}

		$this->unlockNode($id, $schema);
	}

	public function triggerBeforeSet($id, &$node, &$data)
	{
		return true;
	}

	/**
	 * Complete object/array properties
	 *
	 * @param mixed $source
	 * @param mixed $complement
	 * @integer $level
	 *
	 * @return mixed
	 */
	final static function cop(&$source, $complement, $level = 0)
	{
		$null = null;

		if(is_null($source))
		{
			return $complement;
		}

		if(is_null($complement))
		{
			return $source;
		}

		if(is_scalar($source) && is_scalar($complement))
		{
			return $complement;
		}

		if(is_scalar($complement) || is_scalar($source))
		{
			return $source;
		}

		if($level < 100)
		{ // prevent infinite loop
			if(is_object($complement))
			{
				$complement = get_object_vars($complement);
			}

			foreach($complement as $key => $value)
			{
				if(is_object($source))
				{
					if(isset($source->$key))
					{
						$source->$key = self::cop($source->$key, $value, $level + 1);
					}
					else
					{
						$source->$key = self::cop($null, $value, $level + 1);
					}
				}
				if(is_array($source))
				{
					if(isset($source[ $key ]))
					{
						$source[ $key ] = self::cop($source[ $key ], $value, $level + 1);
					}
					else
					{
						$source[ $key ] = self::cop($null, $value, $level + 1);
					}
				}
			}
		}

		return $source;
	}

	public function triggerAfterSet($id, &$old, $new)
	{
		return true;
	}

	public function triggerAfterDel($id, $schema)
	{
		return '';
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
		if(is_null($schema))
		{
			$schema = $this->schema;
		}

		if(file_exists(DIV_NODES_ROOT . $schema . "/$id"))
		{
			if( ! is_dir(DIV_NODES_ROOT . $schema . "/$id"))
			{
				return true;
			}
		}

		return false;
	}

	/**
	 * Insert a node in schema
	 *
	 * @param mixed  $node
	 * @param scalar $id
	 * @param string $schema
	 *
	 * @return mixed
	 */
	public function addNode($node, $id = null, $schema = null)
	{
		if(is_null($schema)) $schema = $this->schema;
		if(is_null($id)) $id = date("Ymdhis") . uniqid();

		if($this->isReservedId($id))
		{
			self::log("Invalid ID '$id' for node");

			return false;
		}

		$node = $this->triggerBeforeAdd($node, $id, $schema);

		if($node == false) return false;

		$data = serialize($node);

		file_put_contents(DIV_NODES_ROOT . $schema . "/$id", $data);

		$this->lockNode($id, $schema);

		$r = $this->triggerAfterAdd($node, $id, $schema);

		if($r === DIV_NODES_ROLLBACK_TRANSACTION)
		{
			unlink(DIV_NODES_ROOT . $schema . "/$id");
			$this->unlockNode($id, $schema);

			return DIV_NODES_ROLLBACK_TRANSACTION;
		}

		$this->unlockNode($id, $schema);

		return $id;
	}

	public function triggerBeforeAdd($node, $id, $schema)
	{
		return $node;
	}

	public function triggerAfterAdd($node, $id, $schema)
	{
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
	public function getRecursiveNodes($schema = "/", $paramsBySchema = [], $paramsDefault = [], $offset = 0, $limit = - 1, $onlyIds = false)
	{
		$schemas = [$schema => $schema];
		$schemas = array_merge($schemas, $this->getSchemas($schema));

		$nodes = [];
		foreach($schemas as $schema)
		{
			$params = $paramsDefault;
			if(isset($paramsBySchema[ $schema ]))
			{
				$params = $paramsBySchema[ $schema ];
				$params = array_merge($paramsDefault, $params);
			}

			$list = $this->getNodes($params, $schema, true);

			if($list !== false) $nodes[ $schema ] = $list;
		}

		// limit result
		$list = [];
		$i    = 0;
		$c    = 0;
		foreach($nodes as $schema => $ids)
		{
			foreach($ids as $id) if($i >= $offset)
			{
				if($c < $limit || $limit == - 1)
				{
					if( ! isset($list[ $schema ]))
					{
						$list[ $schema ] = [];
					}
					if( ! $onlyIds)
					{
						$list[ $schema ][ $id ] = $this->getNode($id, $schema);
					}
					else
					{
						$list[ $schema ][ $id ] = $id;
					}
					$c ++;
				}
			}

			$i ++;
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

		if($this->existsSchema($from))
		{
			$schemas[ $from ] = $from;

			$stack = [$from => $from];

			while(count($stack) > 0) // avoid recursive calls!!
			{
				$from = array_shift($stack);

				$dir = scandir(DIV_NODES_ROOT . $from);

				foreach($dir as $entry)
				{
					$fullSchema = str_replace("//", "/", "$from/$entry");

					if($entry != '.' && $entry != '..' && ! is_file(DIV_NODES_ROOT . $fullSchema))
					{
						$stack[ $fullSchema ]   = $fullSchema;
						$schemas[ $fullSchema ] = $fullSchema;
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
	public function getNodes($params = [], $schema = null, $onlyIds = false)
	{
		if(is_null($schema)) $schema = $this->schema;
		if( ! $this->existsSchema($schema)) return false;

		$dp = [
			"offset" => 0,
			"limit" => - 1,
			"fields" => "*",
			"order" => null,
			"order_asc" => true
		];

		$params = self::cop($dp, $params);
		$ids    = $this->getNodesID($schema);

		// get result
		$newIds = [];

		foreach($ids as $id)
		{
			$node = $this->getNode($id, $schema);

			$vars = [];
			if(is_object($node))
			{
				$vars = get_object_vars($node);
			}
			elseif(is_array($node))
			{
				$vars = $node;
			}
			elseif(is_scalar($node))
			{
				$vars = ['value' => $node];
			}

			$r = true;
			if(isset($params['where']))
			{

				$w = $params['where'];

				foreach($vars as $key => $value)
				{
					$w = str_replace('{' . $key . '}', '$vars["' . $key . '"]', $w);
				}

				$w = str_replace('{id}', '$id', $w);

				$r = false;
				eval('$r = ' . $w . ';');
			}


			if($r === true)
			{

				$newIds[] = $id;
			}
		}

		// sort results
		if(isset($params['order']))
		{
			$order = $params['order'];

			if($order !== false && ! is_null($order))
			{
				$sorted = [];
				foreach($newIds as $id)
				{
					$node          = $this->getNode($id, $schema);
					$sorted[ $id ] = $node;
					if(is_object($node) && isset($node->$order))
					{
						$sorted[ $id ] = $node->$order;
					}
					if(is_array($node) && isset($node[ $order ]))
					{
						$sorted[ $id ] = $node[ $order ];
					}
				}

				if(asort($sorted))
				{
					if($params['order_asc'] === false)
					{
						$sorted = array_reverse($sorted);
					}
					$newIds = $sorted;
				}
			}
		}


		// limit result
		$list = [];
		$i    = 0;
		$c    = 0;
		foreach($newIds as $id)
		{
			if($i >= $params['offset'])
			{
				if($c < $params['limit'] || $params['limit'] == - 1)
				{
					if( ! $onlyIds)
					{
						$list[ $id ] = $this->getNode($id, $schema);
					}
					else
					{
						$list[] = $id;
					}
					$c ++;
				}
			}
			$i ++;
		}

		return $list;
	}

	/**
	 * Return the count of nodes
	 *
	 * @param array  $params
	 * @param string $schema
	 *
	 * @return integer
	 */
	public function getCount($params = [], $schema = null)
	{
		if(is_null($schema))
		{
			$schema = $this->schema;
		}

		if( ! $this->existsSchema($schema))
		{
			return false;
		}

		$dp     = [
			"where" => "true"
		];
		$params = self::cop($dp, $params);

		$ids  = $this->getNodesID($schema);
		$list = [];

		$c = 0;
		foreach($ids as $id)
		{
			$node = $this->getNode($id, $schema);

			if(is_object($node))
			{
				$vars = get_object_vars($node);
			}
			elseif(is_array($node))
			{
				$vars = $node;
			}
			elseif(is_scalar($node))
			{
				$vars = [
					'value' => $node
				];
			}

			$w = $params['where'];
			foreach($vars as $key => $value)
			{
				$w = str_replace('{' . $key . '}', '$vars["' . $key . '"]', $w);
			}
			$w = str_replace('{id}', $id, $w);

			$r = false;
			eval('$r = ' . $w . ';');
			if($r === true)
			{
				$c ++;
			}
		}

		return $c;
	}

	/**
	 * Remove some nodes
	 *
	 * @param array  $params
	 * @param string $schema
	 */
	public function delNodes($params = [], $schema = null)
	{
		if(is_null($schema))
		{
			$schema = $this->schema;
		}

		if( ! $this->existsSchema($schema))
		{
			return false;
		}

		$dp     = [
			"where" => "true",
			"offset" => 0,
			"limit" => - 1
		];
		$params = self::cop($dp, $params);

		if($params['where'] != "true")
		{
			$nodes = $this->getNodes($params, $schema);
			foreach($nodes as $id => $node)
			{
				$this->delNode($id, $schema);
			}
		}
		else
		{
			$nodes = $this->getNodesID($schema);
			foreach($nodes as $id)
			{
				$this->delNode($id, $schema);
			}
		}
	}

	/**
	 * Set id of Node
	 *
	 * @param scalar $id_old
	 * @param scalar $id_new
	 * @param string $schema
	 */
	public function setNodeID($id_old, $id_new, $schema = null)
	{
		if(is_null($schema))
		{
			$schema = $this->schema;
		}

		if( ! $this->existsSchema($schema))
		{
			return false;
		}

		$sec = 0;
		while($this->isLockNode($id_old, $schema) || $sec > 999999)
		{
			$sec ++;
		}

		$this->lockNode($id_old, $schema);
		$this->lockNode($id_new, $schema);

		// Update cascade
		$references = $this->getReferences($schema);
		foreach($references as $rel)
		{
			if($rel['foreign_schema'] == $schema && $rel['update_cascade'] == true)
			{
				$ids = $this->getNodesID($rel['schema']);
				foreach($ids as $fid)
				{
					$node = $this->getNode($fid, $rel['schema']);

					$procede = false;

					if(is_array($node))
					{
						if(isset($node[ $rel['property'] ]))
						{
							if($node[ $rel['property'] ] == $id_old)
							{
								$procede = true;
							}
						}
					}
					elseif(is_object($node))
					{
						if(isset($node->$rel['property']))
						{
							if($node->$rel['property'] == $id_old)
							{
								$procede = true;
							}
						}
					}

					if($procede)
					{
						$this->setNode($fid, [
							$rel['property'] => $id_new
						], $rel['schema']);
					}
				}
			}
		}

		rename(DIV_NODES_ROOT . $schema . "/$id_old", DIV_NODES_ROOT . $schema . "/$id_new");

		$this->unlockNode($id_old, $schema);
		$this->unlockNode($id_new, $schema);
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
		$dp     = [
			"schema" => $this->schema,
			"foreign_schema" => $this->schema,
			"update_cascade" => true,
			"delete_cascade" => true
		];
		$params = self::cop($dp, $params);

		if( ! isset($params['property']))
		{
			return false;
		}

		$schema         = $params['schema'];
		$foreign_schema = $params['foreign_schema'];

		if( ! $this->existsSchema($schema))
		{
			return false;
		}
		if( ! $this->existsSchema($foreign_schema))
		{
			return false;
		}

		$references  = $this->getReferences($schema);
		$freferences = $this->getReferences($foreign_schema);

		foreach($references as $rel)
		{
			if(serialize($rel) == serialize($params))
			{
				return true;
			}
		}

		$references[]  = $params;
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

		if( ! isset($params['property']))
		{
			return false;
		}

		$schema         = $params['schema'];
		$foreign_schema = $params['foreign_schema'];

		$references     = $this->getReferences($schema);
		$new_references = [];
		foreach($references as $rel)
		{
			if($rel['schema'] == $params['schema'] && $rel['foreign_schema'] == $params['foreign_schema'] && $rel['property'] == $params['property'])
			{
				continue;
			}
			$new_references[] = $rel;
		}

		file_put_contents(DIV_NODES_ROOT . $schema . "/.references", serialize($new_references));

		$references     = $this->getReferences($foreign_schema);
		$new_references = [];
		foreach($references as $rel)
		{
			if($rel['schema'] == $params['schema'] && $rel['foreign_schema'] == $params['foreign_schema'] && $rel['property'] == $params['property'])
			{
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
	 * @param string  $schema
	 * @param array   $otherData
	 */
	public function forEachNode($closure, $schema = null, &$otherData = [])
	{
		if(is_null($schema)) $schema = $this->schema;

		if($dir = opendir(DIV_NODES_ROOT . $schema))
		{
			while(($file = readdir($dir)) !== false)
			{
				$full_path = DIV_NODES_ROOT . $schema . "/" . $file;

				if(pathinfo($full_path, PATHINFO_EXTENSION) == "idx" && file_exists(substr($full_path, 0, strlen($full_path) - 4))) continue;

				if( ! $this->isReservedId($file) && $file != "." && $file != "..")
				{
					$node = $this->getNode($file, $schema);
					$md5  = md5(serialize($node));

					$result = $closure($node, $file, $schema, $this, $otherData);

					if($result == DIV_NODES_FOR_BREAK) break;
					if($result == DIV_NODES_FOR_CONTINUE_DISCARDING) continue;

					// default: DIV_NODES_FOR_CONTINUE_SAVING)

					$new_md5 = md5(serialize($node));
					if($md5 != $new_md5) $this->setNode($file, $node, $schema);
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
		for($i = 0; $i < $l; $i ++) if(stripos($chars, $content[ $i ]) !== false) $new_content .= $content[ $i ];
		else $new_content .= ' ';

		$new_content = trim(strtolower($new_content));

		while(strpos($new_content, '  ') !== false) $new_content = str_replace('  ', ' ', $new_content);

		$words = explode(' ', $new_content);

		$new_words = [];
		foreach($words as $word) $new_words[ $word ] = $word;

		return $new_words;
	}

	/**
	 * Add index of node
	 *
	 * @param array  $words
	 * @param scalar $nodeId
	 * @param string $schema
	 * @param null   $indexSchema
	 */
	public function addIndex($words, $nodeId, $schema = null, $indexSchema = null)
	{
		if(is_null($schema)) $schema = $this->schema;
		if(is_null($indexSchema)) $indexSchema = $schema . '/.index';

		$this->addSchema($indexSchema);

		$pathToNode = "$schema/$nodeId";
		$id         = md5($pathToNode);

		foreach($words as $word)
		{
			$l          = strlen($word);
			$wordSchema = '';
			for($i = 0; $i < $l; $i ++) $wordSchema .= $word[ $i ] . '/';

			$wordSchema = "$indexSchema/$wordSchema";

			$this->addSchema($wordSchema);

			$node = $this->getNode($id, $wordSchema, [
				"schema" => $schema,
				"id" => $nodeId,
				"path" => $pathToNode,
				"last_update" => date("Y-m-d h:i:s")
			]);

			if($this->existsNode($id, $wordSchema)) $this->setNode($id, $node, $wordSchema);
			else
				$this->addNode($node, $id, $wordSchema);

			// save reverse index
			$node = $this->getNode("$nodeId.idx", $schema, [
				"indexes" => [],
				"last_update" => date("Y-m-d h:i:s")
			]);

			$node['indexes'][ $wordSchema ] = $id;

			if($this->existsNode("$nodeId.idx", $schema)) $this->setNode("$nodeId.idx", $node, $schema);
			else
				$this->addNode($node, "$nodeId.idx", $schema);
		}
	}

	/**
	 * Create index of schema
	 *
	 * @param closure $contentExtractor
	 * @param string  $schema
	 * @param string  $indexSchema
	 */
	public function createIndex($contentExtractor = null, $schema = null, $indexSchema = null)
	{
		if(is_null($contentExtractor)) $contentExtractor = function($node, $nodeId)
		{
			$content = '';

			if(is_object($node))
			{
				if(method_exists($node, '__toContent')) $content = $node->__toContent();
				elseif(method_exists($node, '__toString')) $content = "$node";
			}
			elseif(is_scalar($node)) $content = "$node";

			return $content;
		};

		// indexing each node
		$this->forEachNode(function($node, $nodeId, $schema, divNodes $db, $otherData)
		{

			$contentExtractor = $otherData['contentExtractor'];
			$indexSchema      = $otherData['indexSchema'];
			$words            = [];
			$extract_words    = true;

			// extract words from built-in node method
			if(is_object($node))
			{
				if(method_exists($node, '__toWords'))
				{
					$extract_words = false;
					$words         = $node->__toWords();
				}
			}

			// get content
			$content = $contentExtractor($node, $nodeId);

			// extract words
			if($extract_words && count($words) == 0) $words = $db->getWords($content);

			$db->addIndex($words, $nodeId, $schema, $indexSchema);

		}, $schema, [
			'indexSchema' => $indexSchema,
			'contentExtractor' => $contentExtractor
		]);
	}

	/**
	 * Full text search
	 *
	 * @param string $phrase
	 * @param string $indexSchema
	 * @param int    $offset
	 * @param int    $limit
	 *
	 * @return array
	 */
	public function search($phrase, $indexSchema = null, $offset = 0, $limit = - 1)
	{
		if(is_null($indexSchema)) $indexSchema = $this->schema . '/.index';

		$results = [];

		$words = $this->getWords($phrase);


		foreach($words as $word)
		{
			// build schema from word
			$l      = strlen($word);
			$schema = '';

			for($i = 0; $i < $l; $i ++) $schema .= $word[ $i ] . "/";

			$schema = "$indexSchema/$schema";

			if($this->existsSchema($schema))
			{
				// get indexes
				$schemas = $this->getRecursiveNodes($schema, [], [], $offset, $limit);

				// calculate score
				foreach($schemas as $sch => $nodes) foreach($nodes as $node)
				{
					$id = md5($node['path']);

					if( ! isset($results[ $id ]))
					{
						$node['score']  = 0;
						$results[ $id ] = $node;
					}

					$results[ $id ]['score'] ++;
				}
			}
		}

		// sort results
		uasort($results, function($a, $b)
		{
			if($a['score'] == $b['score']) return 0;

			return $a['score'] > $b['score'] ? - 1 : 1;
		});

		return $results;
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
		$stats = $this->getNode(".stats", $schema, null);

		if(is_null($stats)) $stats = $this->reStats($schema);

		return $stats;
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
		$stats = [
			'count' => 0
		];

		// 'count' stat
		$this->forEachNode(function($node, $file, $schema, $db, &$stats = [])
		{
			$stats['count'] ++;

			return DIV_NODES_FOR_CONTINUE_DISCARDING;
		}, $schema, $stats);

		// save stat
		$this->setNode('.stats', $stats, $schema);

		return $stats;
	}
}
