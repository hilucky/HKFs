<?
/**
 * ファイルシステム操作クラス
 *
 * @author Kazuhito Hiraki <hiraki@axseed.co.jp>
 * @since 2011-08-19
 */
Class HKFs {
	/**
	 * @var string $ini_filepath 設定ファイルパス
	 * @access private
	 */
	private $ini_filepath = "/data/hdev/app/core/hkfs.ini";
	/**
	 * @var string $replication_lock_filepath レプリケーション・ロックファイル
	 * @access private
	 */
	private $replication_lock_filepath = "/data/hdev/app/bin/repliocation.lock";
	/**
	 * @var string $erase_lock_filepath イレイサー・ロックファイル
	 * @access private
	 */
	private $erase_lock_filepath = "/data/hdev/app/bin/erase.lock";
	/**
	 * @var int $error エラーメッセージ
	 * @access private
	 */
	private $error;
	
	/**
	 * エラーメッセージの取得
	 *
	 * @return string
	 * @access public
	 */
	public function getError() {
		return $this->error;
	}
	/**
	 * エラーメッセージのセット
	 *
	 * @param string error
	 * @return void
	 * @access private
	 */
	private function setError($error) {
		$this->error = $error;
		error_log("HKFs ERROR: ".$this->error);
	}
	
	
	/**
	 * ファイル保存
	 *
	 * @access public
	 * @param string $filepath ファイルパス
	 * @param string $filename ファイル名（ファイルパスに拡張子を含まない場合があるので）
	 * @return mixed file_id or false
	 */
	public function save($filepath, $filename = '') {
		// 保存可能な複数のストレージの情報を得る
		$hosts = self::getHosts();
		if ( count($hosts) > 0  ) {
			// うち、一カ所に保存する
			$host = $hosts[0];
		} else {
			// 利用可能なホストがありません
			$this->setError("There is no available host.");
			return false;
		}
		// ファイルID作成
		$file_id = md5(uniqid(rand(),1));
		// 格納ストレージ（WebDav）のURLを取得
		$url = $this->getUrl($file_id, $host, $filepath, $filename);
		// 格納ストレージ（WebDav）にアップロード
		$Dav = new HKWebDav();
		if ( ! $Dav->upload($url, $filepath) ) {
			// 転送できませんでした
			$this->setError("Can not transfer. $url, ".$Dav->getError());
			return false;
		}
		// ファイルインデックスに登録
		$path = parse_url($url, PHP_URL_PATH);
		$filesize = filesize($filepath);
		if ( ! $this->addFileIndex($file_id, $host['host_id'], $path, $filesize) ) {
			// インデックスに追加できません
			$this->setError("Can not be added to the index.");
			return false;
		}
		
		return $file_id;
	}
	/**
	 * ファイルの取得（URLが返る）
	 *
	 * @access public
	 * @param string $file_id ファイルID
	 * @return string $url URL
	 */
	public function get($file_id) {
		if ( $file_id == '' ) return '';
		$url = $this->getUrl($file_id);
		self::log("Get $url");

		return $url;
	}
	 /**
	  * 複製処理（バッチ）
	  *
	  * @param int $num 複製数
	  * @return void
	  */
	 public function replicate($num) {
	 	// 登録件数が複製数よりも少ないものを抽出する
	 	$files = self::getReplicateFiles($num);
	 	// 対象があった場合
		if ( is_array($files) ) {
			foreach ( $files as $file ) {
				if ( self::isLock($this->replication_lock_filepath) ) {
					error_log("Replicator ERROR: Replicating. ");
					exit;
				}
				// 処理開始
				self::lock($this->replication_lock_filepath);
				
				// 対象ファイルの格納ホストリスト
				$hosts = self::getHostsByFileId($file['file_id']);
				// 登録されていないホストリストを得る
				$unreg_hosts = self::getHosts($hosts);
				// このうち、複製件数分複製を行う
				if ( count($unreg_hosts) > 0 ) {
					// 複製元
					if ( ! ($url = $this->getUrl($file['file_id'])) ) {
						error_log("Replicator ERROR: Can't get url. $url");
						// 複製元のURLが取得できないのでブレイク
						break;
					}
					for ( $i = 0; $i < $num; $i ++ ) {
						if ( $i > count($unreg_hosts) ) {
							// 複製先ホストが、複製数に満たなかった場合
							error_log("Replicator ERROR: Host is not enough. replicates:{$replicates}, unreg_host:".count($unreg_host));
							// 再度ホストが追加されれば次回複製される
							break;
						}
						$dest_host = $unreg_hosts[$i];
						// 複製先のURL作成
						$dest_url = self::makeUrl($dest_host, $hosts[0]['path']);
						// 複製
						self::log("Replicate {$url} > {$dest_url}");

						$Dav = new HKWebDav();
						if ( ! $Dav->upload($dest_url, $url) ) {
							error_log("Replicator ERROR: Can't replicate. {$url} to {$dest_url}");
							// 次回リトライしてもらう
						} else {
							//  成功すればインデックス追加
							if ( ! ($this->addFileIndex($file['file_id'], $dest_host['host_id'], $hosts[0]['path'], $hosts[0]['filesize'])) ) {
								error_log("Replicator ERROR: Can not be added to the index.");
								break;
							}
						}
						sleep(0.5);
					} // for
				} // if 
				self::unlock($this->replication_lock_filepath);
			} // foreach
		}
	 }
	/**
	 * ファイルの削除
	 *
	 * @access public
	 * @param string $file_id
	 * @return bool
	 */
	public function delete($file_id) {
		if ( $file_id == '' ) return false;
		return $this->delFileIndex($file_id);
	}
	/**
	 * ファイルの抹消（バッチ）
	 *
	 * @access public
	 * @return void
	 */
	public function erase() {
		// 削除対象レコードを取得
		$files = self::getEraseFiles();
		// 対象があった場合
		if ( is_array($files) ) {
			if ( self::isLock($this->erase_lock_filepath) ) {
				error_log("Replicator ERROR: Replicating ");
				exit;
			}
			self::lock($this->erase_lock_filepath);
			// 各ホストごとにファイルとインデックスを削除していく
			foreach ( $files as $file ) {
				// 対象ファイルのホスト情報を取得
				$host = $this->getHostInfo($file['host_id']);
				$url = self::makeUrl($host, $file['path']);
				// ディレクトリごと削除してしまうので注意すること
				// ファイルの削除
				self::log("Erase $url");
				$Dav = new HKWebDav();
				if ( ! ( $Dav->delete($url) ) ) {
					error_log("Eraser ERROR: Can't erase. {$url}");
				} else {
					// 成功したらインデックスから削除
					if ( ! ($this->eraseFileIndex($file['id'])) ) {
						error_log("Eraser ERROR: Can not be erase from the index.");
						break;
					}
				}
				sleep(0.5);
			}
			self::unlock($this->erase_lock_filepath);	
		}
	}
	

	/**
	 * 保存に最適なホストを取得
	 *
	 * @access private
	 * @param array $hosts 抽出から除外するホスト情報配列
	 * @return array ホスト情報
	 */
	 private static function getHosts($hosts = '') {
		$query = "SELECT * FROM fs_hosts WHERE status='1'\n";
		if ( is_array($hosts) ) {
			foreach ( $hosts as $host ) {
				if ( $host['host_id'] != '' ) $query .= "and host_id != '".pg_escape_string($host['host_id'])."'\n";
			} 
		}
		$query .= "ORDER BY disk_total - disk_used desc;";
		$Db = new HKFsDb();
		if ( ! ($res = $Db->exec($query)) ) return false;
		$hosts = $Db->res2list($res);

		return $hosts;
	}
	/**
	 * ホスト情報を取得
	 *
	 * @access private
	 * @param string $host_id
	 * @return mixed array or false
	 */
	private static function getHostInfo($host_id) {
		if ( $host_id == '' ) return false;
		$query = "SELECT * FROM fs_hosts WHERE status='1' and host_id = '".pg_escape_string($host_id)."' LIMIT 1;";
		$Db = new HKFsDb();
		if ( ! ($res = $Db->exec($query)) ) return false;
		$list = $Db->res2list($res);
		return $list[0];
	}
	
	/**
	 * URLを取得
	 *
	 * @access private
	 * @param string $file_id
	 * @param array  $host ホスト情報配列
	 * @param string $filepath
	 * @param string $filename
	 * @return mixed $url or false
	 */
	private function getUrl($file_id, $host = '', $filepath = '', $filename = '') {
		if ( $file_id == '' ) return '';
		// 保存するURLと保存されているURLに分岐処理
		if ( $filepath != '' ) {
			// filepathが指定されている場合、保存するパスを取得
			$path = self::getSavePath($file_id, $host['path'], $filepath, $filename);
		} else {
			// 指定が無い場合、ファイルインデックスから情報を取得する
			$query = "SELECT * FROM fs_file_on\n";
			$query .= "WHERE file_id = '".pg_escape_string($file_id)."'\n";
			$query .= "and recdel_ts is null;";
	 		$Db = new HKFsDb();
	 		if ( ! ($res = $Db->exec($query)) ) return false;
			$files = $Db->res2list($res);
			// いくつかのうち、ひとまず先頭のものをチョイス
			$file = $files[0];
			if ( $file['file_id'] == '' ) return false;
			if ( ! ($host = self::getHostInfo($file['host_id'])) ) return false;
			$path = $file['path'];
		}
		$url = self::makeUrl($host, $path);

		return $url;
	}
	/**
	 * URLの生成
	 *
	 * @access private
	 * @param array   $host ホスト情報配列
	 * @param string  $path パス
	 * @return string $url
	 */
	private static function makeUrl($host, $path = '') {
		$url = "http://".$host['hostname'];
		if ( $host['port'] != '' ) $url .= ":".$host['port'];
		if ( $path != '' ) $url .= $path;
		
		return $url;
	}
	/**
	 * 保存するファイルパスを取得
	 *
	 * @access private
	 * @param string  $file_id
	 * @param string  $filepath
	 * @param string  $filename ファイル名（ファイルパスに拡張子を含まない場合があるので）
	 * @return string $filepath
	 */
	private static function getSavePath($file_id, $path, $filepath, $filename = '') {
		if ( $file_id == '' ) return '';
		$ext = strtolower(pathinfo($filepath, PATHINFO_EXTENSION));
		if ( $ext == '' ) $ext = strtolower(pathinfo($filename, PATHINFO_EXTENSION));
		$file = $file_id;
		if ( $ext != '' ) $file .= ".".$ext;
		
		// IDの下３桁を利用
		$a = substr($file_id, -1);
		$b = substr($file_id, -2, 1);
		$c = substr($file_id, -3, 1);
		$path = $path."/".$a."/".$b."/".$c."/".$file;
	
		return $path;
	}
	
	/**
	 * ファイルインデックスへの登録
	 *
	 * @access private
	 * @param string $file_id
	 * @param string $host_id
	 * @param string $path
	 * @return bool
	 */
	private function addFileIndex($file_id, $host_id, $path, $filesize) {
		if ( $file_id == '' || $host_id == '' || $path == '' ) return false;
		$query = "INSERT INTO fs_file_on (id, file_id, host_id, path, filesize, recins_ts, recupd_ts)";
		$query .= " VALUES('".md5(uniqid(rand(),1))."','".pg_escape_string($file_id)."','".pg_escape_string($host_id)."','".pg_escape_string($path)."','".pg_escape_string($filesize)."'";
		$query .= ",'".date("Y-m-d H:i:s")."','".date("Y-m-d H:i:s")."');";
	 	$Db = new HKFsDb();
	 	if ( ! $Db->exec($query) ) return false;
		// ストレージサーバの管理容量を更新
		if ( ! $this->updateDiskSize($host_id, $filesize) ) return false;
		
		return true;
	}
	/**
	 * ファイルインデックスからの削除
	 *
	 * @access private
	 * @param string $file_id
	 * @param string $host_id
	 * @return bool
	 */
	 private function delFileIndex($file_id) {
	 	// 削除日付を入れる
	 	if ( $file_id == '' ) return false;
	 	$query = "UPDATE fs_file_on SET recdel_ts = '".date("Y-m-d H:i:s")."'\n";
	 	$query .= "WHERE file_id = '".pg_escape_string($file_id)."';";
	 	$Db = new HKFsDb();
	 	if ( ! $Db->exec($query) ) return false;
	 	
	 	return true;
	 }
	/**
	 * ファイルインデックスから抹消
	 *
	 * @access private
	 * @param string $id ファイルインデックスのID
	 * @return bool
	 */
	private function eraseFileIndex($id) {
		if ( $id == '' ) return false;
		
		// 該当レコードを取得
		$query = "SELECT * FROM fs_file_on\n";
		$query .= "WHERE id='".pg_escape_string($id)."'\n";
		$query .= "and recdel_ts is not null;";
		$Db = new HKFsDb();
		if ( ! ($res = $Db->exec($query)) ) return false;
		$files = $Db->res2list($res);
		$file = $files[0];
		// 抹消
		if ( $file['id'] == '' ) return false;
		$query = "DELETE FROM fs_file_on\n";
		$query .= "WHERE id='{$file['id']}';";
		if ( ! ($res = $Db->exec($query)) ) return false;
		$filesize = $file['filesize']*-1;
		
		// ストレージサーバの管理容量を更新
		if ( ! $this->updateDiskSize($file['host_id'], $filesize) ) return false;
		
		return true;
	}
	 /**
	  * ストレージサーバの容量を追加
	  *
	  * @access private
	  * @param string $host_id
	  * @param string $filesize 減算する場合はマイナスを入力
	  * @return bool
	  */
	 private function updateDiskSize($host_id, $filesize) {
	 	if ( $host_id == '' || $filesize == '' ) return false;
	 	$query = "UPDATE fs_hosts SET disk_used = disk_used +{$filesize} WHERE host_id = '".pg_escape_string($host_id)."';";
	 	$Db = new HKFsDb();
	 	if ( ! $Db->exec($query) ) return false;
	 	return true;
	 }
	/**
	 * 登録件数が複製数よりも少ないものを抽出する
	 *
	 * @access private
	 * @param int $num 複製数
	 * @return mixed array or false;
	 */
	 private static function getReplicateFiles($num) {
		$num += 1;
		$query = "SELECT file_id FROM (\n";
		$query .= "SELECT file_id, count(*) as cnt FROM fs_file_on WHERE recdel_ts is null GROUP BY file_id\n";
		$query .= ") as a WHERE a.cnt < {$num};";
		$Db = new HKFsDb();
		if ( ! ($res = $Db->exec($query)) ) {
			error_log("Replicator ERROR:".$Db->getError());
			return false;
		}
		
		return $Db->res2list($res);
	 }
	/**
	 * ファイルIDからホストリストを取得
	 *
	 * @access private
	 * @param array $files
	 * @return mixed array or false
	 */
	private static function getHostsByFileId($file_id) {
		if ( $file_id == '' ) return false;
		$query = "SELECT * FROM fs_file_on\n";
		$query .= "WHERE file_id = '".pg_escape_string($file_id)."'\n";
		$query .= "and recdel_ts is null;";
		$Db = new HKFsDb();
		if ( ! ($res = $Db->exec($query)) ) {
			error_log("Replicator ERROR:".$Db->getError());
			return false;
		}
		
		return $Db->res2list($res);
	}
	/**
	 * 削除対象ファイルを取得する
	 *
	 * @access private
	 * @param int $num 複製数
	 * @return mixed array or false;
	 */
	 private static function getEraseFiles() {
		$query = "SELECT * FROM fs_file_on\n";
		$query .= "WHERE recdel_ts is not null;";
		$Db = new HKFsDb();
		if ( ! ($res = $Db->exec($query)) ) {
			error_log("Eraser ERROR:".$Db->getError());
			return false;
		}
		
		return $Db->res2list($res);
	 }
	/**
	 * ロックファイル作成
	 *
	 * @access private
	 * @param string $fid
	 * @return bool
	 */
	private static function lock($filepath) {
		if ( ($fp = fopen($filepath, 'w')) == false ) return false;
		fclose($fp);
		return true;
	}
	/**
	 * ロックファイルの削除
	 *
	 * @access private
	 * @param string $fid
	 * @return bool
	 */
	private static function unlock($filepath) {
		if ( ! unlink($filepath) ) return false;
		return true;
	}
	/**
	 * ロック状態の取得
	 *
	 * @access private
	 * @param string $fid
	 * @return bool
	 */
	private static function isLock($filepath) {
		if ( is_file($filepath) ) return true;
		clearstatcache();
		return false;
	}
	/**
	 * ログ
	 *
	 * @param string $str
	 * @return void
	 */
	public static function log($str) {
		$filepath = LOG_DIR."/hkfs-".date("Ymd").".log";

		//$traces = debug_backtrace(false);

		$line = date("Y/m/d H:i:s")." - ";
		$line .= $str;
		//$line .= " ".$traces[1]['file']." on line ".$traces[1]['line'];
		$line .= "\n";

		$fp = fopen ( $filepath, "a" );
		fputs ( $fp, $line );
		fclose ( $fp );
	}
	 
}
?>