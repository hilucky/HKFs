<?
/**
 * HKFs DB操作クラス
 *
 * @author Kazuhito Hiraki <hiraki@axseed.co.jp>
 * @create 2011-08-24
 */
Class HKFsDb {
	/**
	 * @var string $ini_filepath 設定ファイルパス
	 * @access private
	 */
	private $ini_filepath = "/data/hdev/app/core/HKFs/hkfs.ini";
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
	 * ファイルシステム設定情報の取得
	 *
	 * @return array
	 * @access private
	 */
	private function getConf() {
		if ( ! is_file($this->ini_filepath) ) {
			$this->setError("Not found ini file.".$this->ini_filepath);
			return false;
		}
		return parse_ini_file($this->ini_filepath, true);
	}
	/**
	 * スキームの取得
	 *
	 * @param array パラメータ配列
	 * @return string 
	 */
	private static function scheme($ary) {
		$scheme = "host={$ary['DB']['HKFS_DB_HOST']}";
		$scheme .= " port={$ary['DB']['HKFS_DB_PORT']}";
		$scheme .= " dbname={$ary['DB']['HKFS_DB_NAME']}";
		$scheme .= " user={$ary['DB']['HKFS_DB_USER']}";
		$scheme .= " password={$ary['DB']['HKFS_DB_PASS']}";
		if ( $ary['DB']['HKFS_DB_ENCODE'] != "" ) $scheme .= " options='--client_encoding={$ary['DB']['HKFS_DB_ENCODE']}'";

		return $scheme;
	}
	/**
	 * 接続
	 *
	 * @param string $scheme
	 * @return mixed resource or false
	 */
	private function connect($scheme) {
		if ( $scheme == '' ) return false;
		if ( ! ($conn = pg_connect($scheme)) ) {
			$this->setError("Can't connect DB.");
			return false;
		}
		return $conn;
	}
	/**
	 * 接続のクローズ
	 *
	 * @param resource $conn
	 * @return bool
	 */
	private function close($conn) {
		return pg_close($conn);
	}
	/**
	 * クエリー処理
	 *
	 * @param string $query
	 * @return mixed resource or false
	 */
	private function query($conn, $query) {
		if ( ! ($res = pg_query($conn, $query)) ) {
			$this->setError("DB query ERROR. {$query}");
			return false;
		}
		return $res;
	}
	/**
	 * 実行処理
	 *
	 * @param string $query
	 * @return mixed resource or false
	 */
	public function exec($query) {
		$ini = $this->getConf();
		$scheme = self::scheme($ini);
		if ( ! ($conn = $this->connect($scheme)) ) return false;
		if ( ! ($res = $this->query($conn, $query)) ) return false;
		if ( ! $this->close($conn) ) return false;

		return $res;
	}
	/**
	 * res2list形式へ
	 *
	 * 結果の件数が0の場合はnullが返される
	 * <code>
	 * return 
	 * array(0 => array(key => val,
	 *                  key => val,....),
	 *       1 => array(key => val,
	 *                  key => val,....));
	 * </code>
	 * @param resource $res
	 * @return mixed array or null
	 */
	public function res2list($res) {
		$ret = @pg_fetch_all($res);
		if ($ret == 0 || $ret == false) return null;
		return $ret;
	}
}
?>