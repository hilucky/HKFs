<?
/**
 * WebDav操作クラス
 *
 * @author Kazuhito Hiraki <hiraki@axseed.co.jp>
 * @since 2011-08-18
 */
 
Class HKWebDav {
	/**
	 * @var int $errno エラー番号
	 * @access private
	 */
	private $errno;
	/**
	 * @var int $errno エラーメッセージ
	 * @access private
	 */
	private $error;
	/**
	 * @var string $result 実行応答結果
	 * @access private
	 */
	private $result;
	/**
	 * @var array $info 直近の伝送に関する情報
	 * @access private
	 */
	private $info;
	/**
	 * @var int $http_code HTTPコード
	 * @access private
	 */
	private $http_code;
	
	
	/**
	 * エラー番号の取得
	 *
	 * @return int
	 */
	public function getErrno() {
		return $this->errno;
	}
	/**
	 * エラーメッセージの取得
	 *
	 * @return string
	 */
	public function getError() {
		return $this->error;
	}
	/**
	 * エラーメッセージのセット
	 *
	 * @param string error
	 * @return void
	 */
	public function setError($error) {
		$this->error = $error;
		error_log("HKWebDav ERROR: ".$this->error);
	}
	/**
	 * 実行結果取得
	 *
	 * @return mixed string or false
	 */
	public function getResult() {
		return $this->result;
	}
	/**
	 * 直近の伝送結果の取得
	 *
	 * @return mixed array or string
	 */
	public function getInfo($opt = '') {
		$ret = $this->info;
		if ( $opt != '' ) $ret = $this->info[$opt];
		return $ret;
	}
	/**
	 * HTTPコードの取得
	 *
	 * @return int
	 */
	public function getHttpCode() {
		return $this->getInfo("http_code");
	}
	
	/**
	 * ファイルパスがURLかどうか調べる
	 *
	 * @param string $filepath
	 * @return bool
	 */
	private static function isUrl($filepath) {
		$sheme = parse_url($filepath, PHP_URL_SCHEME);
		if ( $sheme != '' ) return true;
		return false;
	}
	/**
	 * URLのパスをパースする
	 *
	 * @param string url $url
	 * @return array
	 */
	public static function parsePath($url) {
		$path = parse_url($url, PHP_URL_PATH);
		$pathinfo = pathinfo($path);
		$filename = $pathinfo['basename'];
		$list = explode("/", $path);
		$items = array();
		if ( is_array($list) ) {
			foreach ( $list as $item ) {
				if ( $item != '' && $item != $filename ) $items[] = $item;
			}
		}
		return $items;
	}
	/**
	 * ファイルアップロード処理
	 *
	 * @param string $url リソースを作成するURL
	 * @param string $filepath 転送するファイル
	 * @return bool
	 */
	public function upload($url, $filepath) {
		//if ( $url == '' || ! is_file($filepath) ) return false;
		if ( $url == '' ) return false;
		// パス部分と、それ以外の部分を取得
		$paths = self::parsePath($url);
		$domains = explode(parse_url($url, PHP_URL_PATH), $url, 2);
		HKFs::log("UPLOAD $url");
		$flg = 0;
		if ( is_array($paths) ) {
			// コレクションの作成処理
			$str = $domains[0];
			foreach ( $paths as $path ) {
				$str .= "/".$path;
				if ( ! $this->mkcol($str) ) {
					$flg = 1;
					break;
				}
				sleep(0.1);
			}
			if ( $flg == 0 ) {
				if ( ! $this->put($url, $filepath) ) return false;
			}
		}
		if ( $flg != 0 ) return false;
		
		return true;
	}

	/**
	 * PUT処理
	 *
	 * PUT メソッドは、同封されたエンティティを供給される Request-URI の元に保存するように要求する。
	 * リクエストURIに示された場所にリソースを生成しようとします。
	 * この時、新規に生成した場合は201(CREATED)レスポンスを、
	 * また既存のリソースを更新した場合は200(OK)か204(No Content:更新)のレスポンスをそれぞれ返します。 
	 *
	 * @param string $url リソースを作成するURL
	 * @param string $filepath 転送するファイル
	 * @return bool
	 */
	public function put($url, $filepath) {
		//if ( $url == '' || ! is_file($filepath) ) return false;
		if ( $url == '' ) return false;
		
		if ( self::isUrl($filepath) ){
			$headers = get_headers($filepath,1);
			$filesize = $headers['Content-Length'];
		} else if ( is_file($filepath) ) {
			$filesize = filesize($filepath);
		}
		
		$fp = fopen($filepath, 'r');
		if ( $fp == false ) return false;
		
		$ch = curl_init($url);
		curl_setopt($ch, CURLOPT_INFILE, $fp);
		curl_setopt($ch, CURLOPT_INFILESIZE, $filesize);
		curl_setopt($ch, CURLOPT_PUT, true);
		curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
		$this->result = curl_exec($ch);
		$this->info = curl_getinfo($ch);
		$this->errno = curl_errno($ch);
		$this->error = curl_error($ch);
		if ( $this->getHttpCode() != 200 && $this->getHttpCode() != 201 && $this->getHttpCode() != 204 ) {
			HKFs::log("PUT $url ".$this->getHttpCode()." NG");
			$this->setError("Put Error. send to $url HTTP CODE:".$this->getHttpCode());
			return false;
		}
		HKFs::log("PUT $url ".$this->getHttpCode()." OK");
		if ( $this->errno != 0 ) {
			HKFs::log("PUT $url ".$this->getHttpCode()." NG $this->error($this->errno)");
			return false;
		}
		
		curl_close($ch);
		fclose($fp);
		
		return true;
	}
	/**
	 * MKCOL処理
	 *
	 * SUCCESS: 200 OK, 201 Created, 301 Moved Permanently
	 * 既にあるコレクションを作成しようとすると301が返される
	 *
	 * @param string $url
	 * @return bool
	 */
	public function mkcol($url) {
		if ( $url == '' ) return false;
		$ch = curl_init($url);
		curl_setopt($ch, CURLOPT_CUSTOMREQUEST, "MKCOL");
		curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
		$this->result = curl_exec($ch);
		$this->info = curl_getinfo($ch);
		$this->errno = curl_errno($ch);
		$this->error = curl_error($ch);
		
		if ( $this->getHttpCode() != 200 && $this->getHttpCode() != 201 && $this->getHttpCode() != 301 ) {
			HKFs::log("MKCOL $url ".$this->getHttpCode()." NG");
			$this->setError("Mkcol Error HTTP CODE: ".$this->getHttpCode());
			return false;
		}
		HKFs::log("MKCOL $url ".$this->getHttpCode()." OK");
		if ( $this->errno != 0 ) {
			HKFs::log("MKCOL $url ".$this->getHttpCode()." NG $this->error($this->errno)");
			return false;
		}
		curl_close($ch);
		
		return true;
	}
	/**
	 * DELETE処理
	 *
	 * SUCCESS: 200 OK, 204 No Content
	 * 404 Not Found (存在しないものを削除しようとしている）
	 * 204 No Content (リソースがあるコレクションを削除した）
	 *
	 * @param string $url 削除対象URL
	 * @return bool
	 */
	public function delete($url) {
		if ( $url == '' ) return false;
		$ch = curl_init($url);
		curl_setopt($ch, CURLOPT_CUSTOMREQUEST, "DELETE");
		curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
		$this->result = curl_exec($ch);
		$this->info = curl_getinfo($ch);
		$this->errno = curl_errno($ch);
		$this->error = curl_error($ch);
		
		if ( $this->getHttpCode() != 200 && $this->getHttpCode() != 204 && $this->getHttpCode() != 404 ) {
			HKFs::log("DELETE $url ".$this->getHttpCode()." NG");
			$this->setError("Delete Error HTTP CODE: ".$this->getHttpCode());
			return false;
		}
		if ( $this->errno != 0 ) {
			HKFs::log("DELETE $url ".$this->getHttpCode()." NG $this->error($this->errno)");
			return false;
		}
		curl_close($ch);
		HKFs::log("DELETE $url ".$this->getHttpCode()." OK");
		
		return true;
	}
}
?>