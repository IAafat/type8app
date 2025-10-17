<?php
declare(strict_types=1);
require __DIR__ . '/config.php';

function db(): PDO {
  static $pdo = null;
  if ($pdo) return $pdo;
  $pdo = new PDO('sqlite:' . DB_PATH, null, null, [
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
  ]);
  $pdo->exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;');
  return $pdo;
}

function ident(string $s): string {
  $s = trim($s);
  $s = preg_replace('/[^A-Za-z0-9_]+/', '_', $s);
  $s = preg_replace('/_+/', '_', $s);
  $s = trim($s, '_');
  if ($s === '' || preg_match('/^[0-9]/', $s)) $s = 'c_' . $s;
  return $s;
}

function infer_type(array $vals): string {
  $isInt = true; $isNum = true;
  foreach ($vals as $v) {
    if ($v === '' || $v === null) continue;
    if (!preg_match('/^-?\d+$/', $v)) $isInt = false;
    if (!is_numeric($v)) $isNum = false;
    if (!$isInt && !$isNum) break;
  }
  if ($isInt) return 'INTEGER';
  if ($isNum) return 'REAL';
  return 'TEXT';
}

function read_tsv(string $path, int $sample = 200): array {
  $fh = new SplFileObject($path, 'r');
  $fh->setFlags(SplFileObject::READ_AHEAD | SplFileObject::DROP_NEW_LINE);
  $header = null; $rows = []; $n = 0;
  while (!$fh->eof()) {
    $line = $fh->fgets();
    if ($line === '') continue;
    $cols = str_getcsv($line, "\t", '"', '\\');
    if ($header === null) { $header = array_map(fn($h)=>ident($h===''?'col':$h), $cols); continue; }
    if (count($cols) === 1 && $cols[0] === '') continue;
    $rows[] = $cols; if (++$n >= $sample) break;
  }
  return [$header ?? [], $rows];
}

function build_schema(string $table, array $header, array $sampleRows): array {
  $colTypes = []; $colCount = count($header);
  for ($i=0;$i<$colCount;$i++){
    $vals=[]; foreach ($sampleRows as $r){ $vals[]=$r[$i]??''; }
    $colTypes[$header[$i]] = infer_type($vals);
  }
  $pk=[];
  if (in_array('session_id',$header,true) && in_array('split_id',$header,true)) $pk=['session_id','split_id'];
  elseif (in_array('session_id',$header,true)) $pk=['session_id'];
  elseif (in_array('rep_scheme',$header,true)) $pk=['rep_scheme']; // for type8_report
  elseif (in_array('col',$header,true)) $pk=['col'];              // for summary_numeric
  return [$colTypes,$pk];
}

function create_table(string $table, array $colTypes, array $pk): void {
  $colsSql=[]; foreach ($colTypes as $c=>$t){ $colsSql[] = sprintf('"%s" %s',$c,$t); }
  $pkSql = $pk ? ', PRIMARY KEY("' . implode('","',$pk) . '")' : '';
  $sql = sprintf('CREATE TABLE IF NOT EXISTS "%s" (%s%s);',$table,implode(',',$colsSql),$pkSql);
  db()->exec($sql);
  foreach (['session_id','split_id','rep_scheme','col'] as $idx){
    if (array_key_exists($idx,$colTypes) && (empty($pk) || !in_array($idx,$pk,true))){
      db()->exec(sprintf('CREATE INDEX IF NOT EXISTS "idx_%s_%s" ON "%s"("%s");',$table,$idx,$table,$idx));
    }
  }
}

function upsert_rows(string $table, array $header, string $path): void {
  $pdo=db(); $place='(' . implode(',', array_fill(0,count($header),'?')) . ')';
  $pkInfo=$pdo->query("PRAGMA table_info('$table')")->fetchAll();
  $pkCols=array_values(array_map(fn($r)=>$r['name'], array_filter($pkInfo, fn($r)=>$r['pk'])));
  $updateSet = $pkCols ? implode(',', array_map(fn($c)=>'"'.$c.'"=excluded."'.$c.'"', array_diff($header,$pkCols))) : '';
  $sql = $pkCols
    ? sprintf('INSERT OR REPLACE INTO "%s" (%s) VALUES %s ON CONFLICT("%s") DO UPDATE SET %s;',
              $table, '"' . implode('","',$header) . '"', $place, implode('","',$pkCols), $updateSet ?: '"' . $header[0] . '"=excluded."' . $header[0] . '"')
    : sprintf('INSERT OR REPLACE INTO "%s" (%s) VALUES %s;', $table, '"' . implode('","',$header) . '"', $place);
  $stmt=$pdo->prepare($sql);

  $fh=new SplFileObject($path,'r'); $fh->setFlags(SplFileObject::READ_AHEAD|SplFileObject::DROP_NEW_LINE);
  $headerRead=false; $pdo->beginTransaction();
  while(!$fh->eof()){
    $line=$fh->fgets(); if($line==='') continue;
    $cols=str_getcsv($line, "\t", '"', '\\');
    if(!$headerRead){ $headerRead=true; continue; }
    if(count($cols)===0 || (count($cols)===1 && $cols[0]==='')) continue;
    if(count($cols)<count($header)) $cols=array_pad($cols,count($header),null);
    if(count($cols)>count($header)) $cols=array_slice($cols,0,count($header));
    $stmt->execute($cols);
  }
  $pdo->commit();
}
