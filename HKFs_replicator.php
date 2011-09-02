<?
/**
 * HKFs Replicator
 *
 * 複製者、２重起動を禁止
 *
 * @author Kazuhito Hiraki <hiraki@axseed.co.jp>
 * @create 2011-08-24
 */
include_once("/data/hdev/app/bootstrap.php");
error_reporting(E_ERROR | E_WARNING | E_PARSE);

// 複製数（自身を含まない）
$replicates = 1;

// 複製処理
$Fs = new HKFs();
$Fs->replicate($replicates);
exit;

?>