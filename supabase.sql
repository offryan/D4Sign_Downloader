-- Registro de quais documentos cada pessoa baixou (Supabase / PostgreSQL).
-- Guarda só quem baixou, o identificador do documento na D4Sign e quando: sem nome nem conteúdo.

CREATE TABLE downloads (
    usuario_id     TEXT        NOT NULL,  -- id da conta (usuarios.id); 'compartilhado' com o login desligado
    usuario_email  TEXT,
    uuid_doc       TEXT        NOT NULL,  -- uuid do documento na D4Sign
    baixado_em     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (usuario_id, uuid_doc)
);

-- A API pública do Supabase não enxerga a tabela; o app conecta com um usuário próprio
ALTER TABLE downloads ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE downloads FROM anon, authenticated;

-- Usuário de banco do app: só lê e grava nesta tabela (troque a senha)
-- CREATE ROLE d4sign_app WITH LOGIN PASSWORD '<senha forte>' NOINHERIT;
-- GRANT USAGE ON SCHEMA public TO d4sign_app;
-- GRANT SELECT, INSERT, UPDATE ON TABLE downloads TO d4sign_app;
-- CREATE POLICY app_acesso ON downloads FOR ALL TO d4sign_app USING (true) WITH CHECK (true);

-- Contas de login (cadastro com e-mail e senha). A senha fica só como hash (scrypt, gerado pelo app).
CREATE TABLE usuarios (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    nome        TEXT        NOT NULL,
    email       TEXT        NOT NULL UNIQUE CHECK (email = lower(email) AND email LIKE '%@sitawi.net'),
    senha_hash  TEXT        NOT NULL,
    criado_em   TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE usuarios ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE usuarios FROM anon, authenticated;
-- GRANT SELECT, INSERT ON TABLE usuarios TO d4sign_app;
-- CREATE POLICY app_acesso ON usuarios FOR ALL TO d4sign_app USING (true) WITH CHECK (true);

-- Confirmação de e-mail: a conta só entra depois de abrir o link enviado para o e-mail
ALTER TABLE usuarios ADD COLUMN confirmado_em TIMESTAMPTZ;
-- GRANT UPDATE (nome, senha_hash, confirmado_em) ON TABLE usuarios TO d4sign_app;

-- Botão "×" da tag Baixado: a pessoa pode tirar a marcação de um documento
-- GRANT DELETE ON TABLE downloads TO d4sign_app;
