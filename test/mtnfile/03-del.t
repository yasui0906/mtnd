for i in $(seq 1 $CDATA);do
  ${srcdir}/mtnfile -p $PORT -D ${REMOTEPATH[$i]}/data$i
  if [ -f $WBASE/export/${REMOTEPATH[$i]}/data$i ]; then
    assert_ng
  else
    assert_ok
  fi
done
