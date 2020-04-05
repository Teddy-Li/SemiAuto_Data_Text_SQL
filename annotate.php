<!DOCTYPE html>
<html lang="en">
<head>
    <style type="text/css">
    body {background-color:pink}
    p {color:black}
    </style>
    <meta charset="UTF-8">
    <title>Annotate</title>
    <script>
        function validateForm(){
            var stu_id=document.forms["annotation"]["stu_id"].value;
            if (stu_id.length != 10){
                alert("学号必须为10位数字！")
                return false;
            }
            for (var i=0;i<stu_id.length;i++)
            {
                if (stu_id[i] > '9' || stu_id[i] < '0'){
                    alert("学号必须为10位数字！")
                    return false
                }
            }
            return true
        }
    </script>
</head>

<body>
<h2>Want to return to Instructions?</h2>
<input type="button" value="返回查看说明" onclick="javascrtpt:window.location.href='./index.html'" />
<hr>

<h2>Write down how you would ask this question and check on the radio boxes below</h2>
<hr>
<?php
$myfile = fopen("SPIDER_input.csv", "r");
$headers = fgets($myfile);
$annotated_file = fopen("spider_annotated.json", "r");
$annotated_str = fgets($annotated_file);
$annotated = json_decode($annotated_str, true);
$entry;
while(! feof($myfile))
{
    $temp_array = fgetcsv($myfile);
    if(! in_array($temp_array[4], $annotated)){
        $entry = $temp_array;
        $annotated[count($annotated)] = $temp_array[4];
        setcookie("gold",$temp_array[3]);
        setcookie("entry_index",$temp_array[4]);
        break;
    }
}
fclose($myfile);
fclose($annotated_file);

if(is_null($entry)){
    die("Files exausted!");
}
else{
    echo "<p><strong>Topic: </strong>", $entry[0], "</p>";
    echo "<p><strong>Question Sequence: </strong><br>",$entry[1],"</p>";
    echo "<p><strong>Answer Sample: </strong><br>",$entry[2],"</p>";
}
?>

<hr>
<form action="saveData.php" method="post" name="annotation" onsubmit="return validateForm()" id="annotation">
    <br>
    Question: <input type="text" style="height:80px;width:700px" name="question" id="question" placeholder="Type how you would ask this question here..." required>
    <br><br>
    <hr>
    <strong>Indicate whether you agree with the following statements:</strong>
    <p>1) The question sequence does not have redundant or meaningless parts.</p>
    <input type="radio" name="concise" value="yes" required>yes<br>
    <input type="radio" name="concise" value="no" required>no
    <p>2) The answer this question sequence asks for has sensible meanings.</p>
    <input type="radio" name="sensible" value="yes" required>yes<br>
    <input type="radio" name="sensible" value="no" required>no
    <p>3) People usually ask such questions in real world.</p>
    <input type="radio" name="worldly" value="yes" required>yes<br>
    <input type="radio" name="worldly" value="no" required>no
    <p>4) This is a complex question.</p>
    <input type="radio" name="complex" value="yes" required>yes<br>
    <input type="radio" name="complex" value="no" required>no
    <hr>
    你的学号（将通过北京大学付费至您入学时校方发放的农行卡 ^_^ ）：<input type="text" name="stu_id" id="stu_id" required>
    <br>
    <input type="submit" value="Submit">
</form>
</body>
</html>