globalThis.fetch = async () => {
  throw new Error('fixture mode must not call fetch');
};
