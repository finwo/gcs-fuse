module.exports = function (st) {
  return {
    mtime: st.mtime || new Date(),
    atime: st.atime || new Date(),
    ctime: st.ctime || new Date(),
    size: st.size !== undefined ? st.size : 0,
    mode: st.mode === 'dir' ? 16832 : (st.mode === 'file' ? 33152 : (st.mode === 'link' ? 41471 : st.mode)),
    uid: st.uid !== undefined ? st.uid : process.getuid(),
    gid: st.gid !== undefined ? st.gid : process.getgid()
  }
};
