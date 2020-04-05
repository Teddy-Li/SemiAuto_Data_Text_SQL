<html>
<body>
<?php
$json = $_POST;
$json["entry_idx"] = $_COOKIE["entry_index"];
$json["gold"] = $_COOKIE["gold"];
$annotated_file = fopen("spider_annotated.json", "r");
$annotated_str = fgets($annotated_file);
fclose($annotated_file);
$annotated = json_decode($annotated_str, true);
$annotated[count($annotated)] = $_COOKIE["entry_index"];
$new_annotated_str = json_encode($annotated);
$new_annotated_file = fopen("spider_annotated.json", "w");
fwrite($new_annotated_file, $new_annotated_str);
fclose($new_annotated_file);

$txt = json_encode($json)."\n";
$fname = "./annotation_result/data_".date("y_m_d_h_i_sa").".json";
$myfile = fopen($fname, "w");
fwrite($myfile, $txt);
fclose($myfile);
echo "<h2>Data Saved!</h2>";
echo "Written question: ", $json["question"], "\n";
echo "Gold question: ", $json["gold"], "\n";
?>
<hr>
<input type="button" value="继续标注" onclick="javascrtpt:window.location.href='./annotate.php'" />
<input type="button" value="返回查看说明" onclick="javascrtpt:window.location.href='./index.html'" />
</body>
</html>