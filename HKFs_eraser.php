<?
/**
 * HKFs Eraser
 *
 * 削除者、２重起動禁止
 *
 * @author Kazuhito Hiraki <hiraki@axseed.co.jp>
 * @create 2011-08-29
 */
include_once("/data/hdev/app/bootstrap.php");
error_reporting(E_ERROR | E_WARNING | E_PARSE);

// 抹消処理
$Fs = new HKFs();
$Fs->erase();

exit;
?>