import discord
from discord.ext import commands
import json
import os
import asyncio
import sys
import traceback
import re
import aiohttp
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from dotenv import load_dotenv

try:
    import psycopg2
    import psycopg2.extras
    _PSYCOPG2_AVAILABLE = True
except ImportError:
    _PSYCOPG2_AVAILABLE = False

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
PREFIX = "yov!"
EMBED_COLOR = 0x7c3aed
DELETE_TIMEOUT = 10
SPAM_LIMIT = 7
SPAM_WINDOW = 5
RAID_THRESHOLD = 5
RAID_WINDOW = 10
BAN_LIMIT_RESET_DAYS = 7
DISCONNECT_LIMIT = 3
DISCONNECT_WINDOW = 60

# ─── CONFIGURACOES DO DONO DO BOT ────────────────────────────────────────────
# Coloque aqui o seu ID do Discord (o dono do bot, nao o dono do servidor)
BOT_OWNER_ID = 1513589643621433402  # yovposse

# Chave secreta para ativar o bot em um novo servidor
# NAO adicione esta chave no help nem em nenhum lugar publico
_ACTIVATION_KEY = "feelingcherishe"

# ID do bot autorizado a solicitar desativacao de servidores
DEACTIVATE_BOT_ID = 1515404435394793472

# Link do servidor de suporte
SUPPORT_SERVER = "https://discord.gg/RyYZAJkw6k"

# ─── Database ────────────────────────────────────────────────────────────────

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.json")
DATABASE_URL = os.getenv("DATABASE_URL")

DEFAULT_ANTIRAID = {
    "anti_spam": True,
    "anti_gore": False,
    "anti_raid": True,
    "anti_disconnect": True,
}

# ─── PostgreSQL helpers ───────────────────────────────────────────────────────

def _pg_connect():
    """Abre uma conexao com o PostgreSQL usando DATABASE_URL."""
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

def init_db_postgres():
    """
    Cria a tabela guild_data (uma linha por servidor) se nao existir.
    Migra dados da tabela antiga bot_data automaticamente, se houver.
    """
    if not DATABASE_URL or not _PSYCOPG2_AVAILABLE:
        return
    try:
        conn = _pg_connect()
        cur = conn.cursor()

        # Tabela principal: uma linha por servidor
        cur.execute("""
            CREATE TABLE IF NOT EXISTS guild_data (
                guild_id    TEXT        PRIMARY KEY,
                guild_name  TEXT,
                data        JSONB       NOT NULL DEFAULT '{}'::jsonb,
                created_at  TIMESTAMPTZ DEFAULT NOW(),
                updated_at  TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        # Migra dados da tabela antiga (bot_data) se existir
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'bot_data'
            )
        """)
        if cur.fetchone()["exists"]:
            cur.execute("SELECT data FROM bot_data WHERE id = 1")
            row = cur.fetchone()
            if row and row["data"] and "guilds" in row["data"]:
                guilds = row["data"]["guilds"]
                for gid, gdata in guilds.items():
                    cur.execute("""
                        INSERT INTO guild_data (guild_id, data)
                        VALUES (%s, %s)
                        ON CONFLICT (guild_id) DO NOTHING
                    """, (gid, json.dumps(gdata, ensure_ascii=False)))
                if guilds:
                    print(f"[DB] Migrados {len(guilds)} servidores da tabela antiga.", flush=True)

        # Tabela de punições (mute/castigo/mutecall com expiração)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS punishments (
                id          SERIAL      PRIMARY KEY,
                guild_id    TEXT        NOT NULL,
                user_id     TEXT        NOT NULL,
                type        TEXT        NOT NULL,
                role_id     TEXT,
                expires_at  TIMESTAMPTZ,
                moderator_id TEXT,
                reason      TEXT,
                active      BOOLEAN     DEFAULT TRUE,
                created_at  TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_punishments_guild_user ON punishments(guild_id, user_id, active)")

        # Tabela de posts do Instagram
        cur.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                id          SERIAL      PRIMARY KEY,
                guild_id    TEXT        NOT NULL,
                channel_id  TEXT        NOT NULL,
                message_id  TEXT,
                author_id   TEXT        NOT NULL,
                image_url   TEXT        NOT NULL,
                caption     TEXT,
                created_at  TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        # Tabela de sorteios
        cur.execute("""
            CREATE TABLE IF NOT EXISTS giveaways (
                id          SERIAL      PRIMARY KEY,
                guild_id    TEXT        NOT NULL,
                channel_id  TEXT        NOT NULL,
                message_id  TEXT,
                host_id     TEXT        NOT NULL,
                prize       TEXT        NOT NULL,
                winners_count INT       DEFAULT 1,
                ends_at     TIMESTAMPTZ NOT NULL,
                ended       BOOLEAN     DEFAULT FALSE,
                winners     TEXT[]      DEFAULT '{}',
                entries     TEXT[]      DEFAULT '{}',
                created_at  TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_giveaways_active ON giveaways(guild_id, ended)")

        conn.commit()
        cur.close()
        conn.close()
        print("[DB] PostgreSQL conectado. Tabelas prontas.", flush=True)
    except Exception as e:
        print(f"[ERRO] init_db_postgres: {e}", flush=True)

def pg_register_guild(guild_id: int, guild_name: str):
    """Registra ou atualiza o nome de um servidor na tabela guild_data."""
    if not DATABASE_URL or not _PSYCOPG2_AVAILABLE:
        return
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO guild_data (guild_id, guild_name, data)
            VALUES (%s, %s, '{}'::jsonb)
            ON CONFLICT (guild_id) DO UPDATE
                SET guild_name = EXCLUDED.guild_name,
                    updated_at = NOW()
        """, (str(guild_id), guild_name))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[ERRO] pg_register_guild: {e}", flush=True)

# ─── Dados padrao de um servidor ─────────────────────────────────────────────

def _make_default_guild_data():
    return {
        "log_channels": {},
        "vip_users": {},
        "vip_perm_users": [],
        "vip_perm_role_id": None,
        "vip_above_role_id": None,
        "vip_manager_roles": [],
        "blacklist": {},
        "anti_ban_users": {},
        "ban_limits": {},
        "vanity_url": None,
        "owner_id": None,
        "prefix": PREFIX,
        "embed_color": None,
        "protected_users": [],
        "antiraid_settings": dict(DEFAULT_ANTIRAID),
        "activated": False,
        "min_role_id": None,
        "ban_roles": [],
        "panel_roles": [],
        "panel_perm_roles": [],
        "owner_perm_roles": [],
        "cl_roles": [],
        "cl_palavras": [],
        "ticket_config": {
            "title": "Suporte — Abrir Ticket",
            "description": "Clique no botao abaixo para abrir um ticket.\nNossa equipe respondera em breve.",
            "open_description": None,
            "category_id": None,
            "support_role_ids": [],
            "assume_role_ids": [],
            "counter": 0,
            "options": [],
        },
        "welcome_config": {
            "enabled": False,
            "channel_id": None,
            "title": "Bem-vindo(a) ao servidor!",
            "description": "Olá, {user}! Seja bem-vindo(a) ao **{server}**!\nVocê é o membro de número **{count}**.",
            "color": None,
        },
        # ── Novos campos ──────────────────────────────────────────────────────
        "staff_roles": [],
        "mute_role_id": None,
        "castigo_role_id": None,
        "mute_call_role_id": None,
        "antiban_role_id": None,
        "instagram_channel_id": None,
        "instagram_roles": [],
        "giveaway_manager_roles": [],
    }

def _apply_guild_defaults(gd: dict):
    """Garante que todos os campos obrigatorios existam. Retorna (gd, changed)."""
    changed = False
    defaults = [
        ("prefix", PREFIX),
        ("embed_color", None),
        ("protected_users", []),
        ("antiraid_settings", dict(DEFAULT_ANTIRAID)),
        ("activated", False),
        ("min_role_id", None),
        ("ban_roles", []),
        ("panel_roles", []),
        ("panel_perm_roles", []),
        ("owner_perm_roles", []),
        ("vip_perm_role_id", None),
        ("vip_above_role_id", None),
        ("vip_manager_roles", []),
        ("cl_roles", []),
        ("cl_palavras", []),
        ("ticket_config", {
            "title": "Suporte — Abrir Ticket",
            "description": "Clique no botao abaixo para abrir um ticket.\nNossa equipe respondera em breve.",
            "open_description": None,
            "category_id": None,
            "support_role_ids": [],
            "assume_role_ids": [],
            "counter": 0,
            "options": [],
        }),
    ]
    for key, default in defaults:
        if key not in gd:
            gd[key] = default
            changed = True
    ar = gd.setdefault("antiraid_settings", dict(DEFAULT_ANTIRAID))
    for k, v in DEFAULT_ANTIRAID.items():
        if k not in ar:
            ar[k] = v
            changed = True
    tc = gd.setdefault("ticket_config", {})
    for tk, tv in [("assume_role_ids", []), ("open_description", None)]:
        if tk not in tc:
            tc[tk] = tv
            changed = True
    if "welcome_config" not in gd:
        gd["welcome_config"] = {
            "enabled": False,
            "channel_id": None,
            "title": "Bem-vindo(a) ao servidor!",
            "description": "Olá, {user}! Seja bem-vindo(a) ao **{server}**!\nVocê é o membro de número **{count}**.",
            "color": None,
        }
        changed = True
    else:
        wc = gd["welcome_config"]
        for wk, wv in [
            ("enabled", False), ("channel_id", None),
            ("title", "Bem-vindo(a) ao servidor!"),
            ("description", "Olá, {user}! Seja bem-vindo(a) ao **{server}**!\nVocê é o membro de número **{count}**."),
            ("color", None),
        ]:
            if wk not in wc:
                wc[wk] = wv
                changed = True
    # Novos campos
    for nk, nv in [
        ("staff_roles", []),
        ("mute_role_id", None),
        ("castigo_role_id", None),
        ("mute_call_role_id", None),
        ("antiban_role_id", None),
        ("instagram_channel_id", None),
        ("instagram_roles", []),
        ("giveaway_manager_roles", []),
    ]:
        if nk not in gd:
            gd[nk] = nv
            changed = True
    return gd, changed

# ─── load_db / save_db (fallback arquivo local) ───────────────────────────────

def load_db():
    """Carrega o JSON local (usado apenas no fallback sem DATABASE_URL)."""
    if not os.path.exists(DB_PATH):
        save_db({"guilds": {}})
    try:
        with open(DB_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        print("[AVISO] data.json corrompido — fazendo backup e recriando...", flush=True)
        backup = DB_PATH + ".bak"
        try:
            if os.path.exists(DB_PATH):
                import shutil
                shutil.copy2(DB_PATH, backup)
                print(f"[AVISO] Backup salvo em: {backup}", flush=True)
        except Exception:
            pass
        data = {"guilds": {}}
        save_db(data)
        return data

def save_db(data):
    """Salva o JSON local (usado apenas no fallback sem DATABASE_URL)."""
    try:
        tmp = DB_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, DB_PATH)
    except IOError as e:
        print(f"[ERRO] Falha ao salvar banco: {e}", flush=True)

def get_guild_data(guild_id):
    gid = str(guild_id)

    # ── PostgreSQL: uma linha por servidor ──
    if DATABASE_URL and _PSYCOPG2_AVAILABLE:
        try:
            conn = _pg_connect()
            cur = conn.cursor()
            cur.execute("SELECT data FROM guild_data WHERE guild_id = %s", (gid,))
            row = cur.fetchone()
            if not row:
                default = _make_default_guild_data()
                cur.execute("""
                    INSERT INTO guild_data (guild_id, data)
                    VALUES (%s, %s)
                    ON CONFLICT (guild_id) DO NOTHING
                """, (gid, json.dumps(default, ensure_ascii=False)))
                conn.commit()
                cur.close()
                conn.close()
                return default
            gd = dict(row["data"])
            cur.close()
            conn.close()
            gd, changed = _apply_guild_defaults(gd)
            if changed:
                update_guild_data(guild_id, gd)
            return gd
        except Exception as e:
            print(f"[ERRO] get_guild_data (postgres): {e}", flush=True)
            return _make_default_guild_data()

    # ── JSON local (fallback sem DATABASE_URL) ──
    db = load_db()
    if gid not in db["guilds"]:
        db["guilds"][gid] = _make_default_guild_data()
        save_db(db)
    else:
        gd, changed = _apply_guild_defaults(db["guilds"][gid])
        db["guilds"][gid] = gd
        if changed:
            save_db(db)
    return db["guilds"][gid]

def update_guild_data(guild_id, data):
    gid = str(guild_id)

    # ── PostgreSQL ──
    if DATABASE_URL and _PSYCOPG2_AVAILABLE:
        try:
            conn = _pg_connect()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO guild_data (guild_id, data, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (guild_id) DO UPDATE
                    SET data = EXCLUDED.data,
                        updated_at = NOW()
            """, (gid, json.dumps(data, ensure_ascii=False)))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"[ERRO] update_guild_data (postgres): {e}", flush=True)
        return

    # ── JSON local ──
    db = load_db()
    db["guilds"][gid] = data
    save_db(db)

def get_ar(guild_id, key):
    gd = get_guild_data(guild_id)
    return gd.get("antiraid_settings", DEFAULT_ANTIRAID).get(key, DEFAULT_ANTIRAID[key])

def toggle_ar(guild_id, key):
    gd = get_guild_data(guild_id)
    ar = gd.setdefault("antiraid_settings", dict(DEFAULT_ANTIRAID))
    ar[key] = not ar.get(key, DEFAULT_ANTIRAID[key])
    update_guild_data(guild_id, gd)
    return ar[key]

# ─── Embed Helper ─────────────────────────────────────────────────────────────

def get_guild_color(guild):
    if guild:
        gd = get_guild_data(guild.id)
        saved_color = gd.get("embed_color")
        if saved_color:
            return int(saved_color, 16) if isinstance(saved_color, str) else saved_color
        color = getattr(guild, 'accent_color', None) or getattr(guild, 'accent_colour', None)
        if color:
            return color.value
    return EMBED_COLOR

def create_embed(guild=None, title=None, description=None):
    color = get_guild_color(guild)
    embed = discord.Embed(color=color, timestamp=datetime.now(timezone.utc))
    if title:
        embed.title = title
    if description:
        embed.description = description
    if guild:
        if guild.icon:
            embed.set_thumbnail(url=guild.icon.url)
        if guild.banner:
            embed.set_image(url=guild.banner.url)
    return embed

def error_embed(description, guild=None):
    return create_embed(guild, title="Erro", description=description)

def success_embed(title, description, guild=None):
    return create_embed(guild, title=title, description=description)

# ─── Log Channels ─────────────────────────────────────────────────────────────

LOG_CHANNELS = {
    "ban":            "log-banimento",
    "mute":           "log-mute",
    "castigo":        "log-castigo",
    "kick":           "log-kick",
    "nuke":           "log-nuke",
    "voice_join":     "log-entrada-call",
    "voice_leave":    "log-saida-call",
    "voice_mute":     "log-mute-call",
    "anti_raid":      "log-anti-raid",
    "anti_spam":      "log-anti-spam",
    "anti_gore":      "log-anti-gore",
    "anti_disconnect":"log-anti-disconnect",
    "bot_join":       "log-entrada-bots",
    "commands":       "log-uso-comandos",
    "blacklist":      "log-blacklist",
    "role_create":    "log-criacao-cargos",
    "role_delete":    "log-exclusao-cargos",
    "channel_create": "log-criacao-canais",
    "channel_delete": "log-exclusao-canais",
    "server_update":  "log-alteracoes-servidor",
    "url_update":     "log-alteracoes-url",
    "members":        "log-entrada-saida-membros",
    "message_edit":   "log-edicao-mensagens",
    "message_delete": "log-exclusao-mensagens",
    "ticket":         "log-tickets",
}

_pending_bot_actions = set()

async def send_log(guild, log_type, embed):
    try:
        gd = get_guild_data(guild.id)
        channel_id = gd["log_channels"].get(log_type)
        channel = None
        if channel_id:
            channel = guild.get_channel(int(channel_id))
        if not channel:
            channel_name = LOG_CHANNELS.get(log_type)
            if channel_name:
                channel = discord.utils.get(guild.text_channels, name=channel_name)
                if channel:
                    gd["log_channels"][log_type] = str(channel.id)
                    update_guild_data(guild.id, gd)
        if channel:
            await channel.send(embed=embed)
    except Exception as e:
        print(f"[ERRO] send_log({log_type}): {e}", flush=True)

# ─── Role / Permission Helpers ────────────────────────────────────────────────

def member_below_bot(guild, member):
    """Retorna True se o cargo mais alto do membro e menor que o do bot."""
    if member is None:
        return True
    if member.id == guild.owner_id:
        return False
    bot_top = guild.me.top_role
    return member.top_role < bot_top

def is_server_owner(ctx):
    if ctx.author.id == ctx.guild.owner_id:
        return True
    gd = get_guild_data(ctx.guild.id)
    owner_perm_roles = gd.get("owner_perm_roles", [])
    if not owner_perm_roles:
        return False
    author_role_ids = {str(r.id) for r in ctx.author.roles}
    return bool(author_role_ids & set(owner_perm_roles))

def is_bot_owner(user_id):
    return user_id == BOT_OWNER_ID

def has_min_perm(guild, member):
    """
    Verifica se o membro tem o cargo minimo para usar comandos.
    Se nenhum cargo minimo estiver definido, todos podem usar.
    O dono do servidor sempre passa.
    """
    if member.id == guild.owner_id:
        return True
    gd = get_guild_data(guild.id)
    min_role_id = gd.get("min_role_id")
    if not min_role_id:
        return True
    min_role = guild.get_role(int(min_role_id))
    if not min_role:
        return True
    return member.top_role >= min_role

def can_ban(guild, member):
    """Retorna True se o membro pode usar o comando de ban."""
    if member.id == guild.owner_id:
        return True
    if member.guild_permissions.ban_members or member.guild_permissions.administrator:
        return True
    gd = get_guild_data(guild.id)
    ban_roles = gd.get("ban_roles", [])
    return any(str(r.id) in ban_roles for r in member.roles)

# ─── Ativacao do Servidor ─────────────────────────────────────────────────────

def is_guild_activated(guild_id):
    gd = get_guild_data(guild_id)
    return gd.get("activated", False)

def activate_guild(guild_id):
    gd = get_guild_data(guild_id)
    gd["activated"] = True
    update_guild_data(guild_id, gd)

def deactivate_guild(guild_id):
    gd = get_guild_data(guild_id)
    gd["activated"] = False
    update_guild_data(guild_id, gd)

# Comandos que NAO precisam de ativacao
_NO_AUTH_COMMANDS = {"ativar", "painel"}

async def check_activation(ctx):
    """
    Verifica se o servidor esta ativado e se o membro tem permissao minima.
    Retorna True se pode executar, False se bloqueado.
    """
    if not ctx.guild:
        return True
    if ctx.command and ctx.command.name in _NO_AUTH_COMMANDS:
        return True
    gd = get_guild_data(ctx.guild.id)
    if not gd.get("activated", False):
        p = gd.get("prefix", PREFIX)
        embed = error_embed(
            f"Este servidor ainda nao esta ativado.\n\n"
            f"Para ativar, o dono do servidor deve digitar:\n"
            f"**`{p}ativar <chave>`**\n\n"
            f"Solicite a chave ao dono do bot.",
            ctx.guild
        )
        embed.title = "Servidor Nao Ativado"
        try:
            msg = await ctx.reply(embed=embed)
            asyncio.create_task(delete_after(msg))
            await ctx.message.delete()
        except Exception:
            pass
        return False
    if not has_min_perm(ctx.guild, ctx.author):
        min_role_id = gd.get("min_role_id")
        min_role = ctx.guild.get_role(int(min_role_id)) if min_role_id else None
        role_name = min_role.name if min_role else "definido"
        embed = error_embed(
            f"Voce nao tem permissao para usar comandos.\n"
            f"Cargo minimo exigido: **{role_name}**",
            ctx.guild
        )
        try:
            msg = await ctx.reply(embed=embed)
            asyncio.create_task(delete_after(msg))
            await ctx.message.delete()
        except Exception:
            pass
        return False
    return True

# ─── Painel de Aprovacao (Dono do Bot) ───────────────────────────────────────

class ApprovalView(discord.ui.View):
    def __init__(self, guild_id, guild_name, requestor_name, requestor_id):
        super().__init__(timeout=None)
        self.guild_id = guild_id
        self.guild_name = guild_name
        self.requestor_name = requestor_name
        self.requestor_id = requestor_id

    async def _finalize(self, interaction, approved):
        for child in self.children:
            child.disabled = True
        result = "Aprovado" if approved else "Negado"
        embed = interaction.message.embeds[0] if interaction.message.embeds else discord.Embed()
        embed.color = 0x000000
        embed.title = f"Solicitacao {result}"
        await interaction.message.edit(embed=embed, view=self)

    @discord.ui.button(label="Aprovar", style=discord.ButtonStyle.secondary)
    async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != BOT_OWNER_ID:
            return await interaction.response.send_message("Apenas o dono do bot pode usar este painel.", ephemeral=True)
        await interaction.response.defer()
        activate_guild(self.guild_id)
        await self._finalize(interaction, True)

        guild = interaction.client.get_guild(self.guild_id)
        if guild:
            # Tenta notificar no primeiro canal de texto disponivel
            notify_channel = None
            try:
                requestor = guild.get_member(self.requestor_id)
                if requestor:
                    notify_channel = requestor
                else:
                    notify_channel = next((ch for ch in guild.text_channels if ch.permissions_for(guild.me).send_messages), None)
            except Exception:
                notify_channel = next((ch for ch in guild.text_channels if ch.permissions_for(guild.me).send_messages), None)

            if notify_channel:
                try:
                    embed = create_embed(guild, "Bot Ativado!", f"O servidor **{guild.name}** foi aprovado!\nO bot esta pronto para uso.")
                    await notify_channel.send(embed=embed)
                except Exception:
                    pass

        await interaction.followup.send(f"Servidor **{self.guild_name}** aprovado com sucesso.", ephemeral=True)

    @discord.ui.button(label="Negar", style=discord.ButtonStyle.secondary)
    async def deny(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != BOT_OWNER_ID:
            return await interaction.response.send_message("Apenas o dono do bot pode usar este painel.", ephemeral=True)
        await interaction.response.defer()
        await self._finalize(interaction, False)

        guild = interaction.client.get_guild(self.guild_id)
        if guild:
            try:
                requestor = guild.get_member(self.requestor_id)
                notify_ch = next((ch for ch in guild.text_channels if ch.permissions_for(guild.me).send_messages), None)
                if notify_ch:
                    embed = error_embed("O bot foi removido pois o acesso nao foi aprovado.", guild)
                    embed.title = "Acesso Negado"
                    await notify_ch.send(embed=embed)
            except Exception:
                pass
            try:
                await guild.leave()
            except Exception:
                pass

        await interaction.followup.send(f"Servidor **{self.guild_name}** negado. Bot saiu do servidor.", ephemeral=True)

# ─── Painel do Dono do Bot (DM) ──────────────────────────────────────────────

GUILDS_PER_PAGE = 5

class GuildRemoveButton(discord.ui.Button):
    def __init__(self, guild_id: int, guild_name: str, page: int):
        super().__init__(
            label="Remover",
            style=discord.ButtonStyle.danger,
            custom_id=f"remove_guild_{guild_id}",
            row=(page % GUILDS_PER_PAGE),
        )
        self.guild_id = guild_id
        self.guild_name = guild_name

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != BOT_OWNER_ID:
            return await interaction.response.send_message("Apenas o dono do bot.", ephemeral=True)
        await interaction.response.defer()
        deactivate_guild(self.guild_id)
        self.disabled = True
        self.label = "Removido"
        self.style = discord.ButtonStyle.secondary

        now = int(datetime.now(timezone.utc).timestamp())
        guild = interaction.client.get_guild(self.guild_id)
        if guild:
            try:
                notify_ch = next(
                    (ch for ch in guild.text_channels if ch.permissions_for(guild.me).send_messages), None
                )
                if notify_ch:
                    embed = discord.Embed(
                        title="Acesso Removido",
                        description=(
                            "O acesso do bot neste servidor foi removido pelo dono do bot.\n"
                            "Para reativar, o dono do servidor deve usar o comando de ativacao novamente."
                        ),
                        color=0x000000,
                        timestamp=datetime.now(timezone.utc)
                    )
                    await notify_ch.send(embed=embed)
            except Exception:
                pass

        try:
            await interaction.message.edit(view=self.view)
        except Exception:
            pass
        await interaction.followup.send(
            f"Permissao do servidor **{self.guild_name}** removida. O dono tera que refazer o procedimento de ativacao.",
            ephemeral=True
        )


def build_guild_panel_embed_and_view(guilds_list: list, page: int = 0) -> tuple:
    total = len(guilds_list)
    total_pages = max(1, (total + GUILDS_PER_PAGE - 1) // GUILDS_PER_PAGE)
    page = max(0, min(page, total_pages - 1))
    start = page * GUILDS_PER_PAGE
    chunk = guilds_list[start:start + GUILDS_PER_PAGE]

    embed = discord.Embed(
        title="Painel de Servidores",
        color=0x000000,
        timestamp=datetime.now(timezone.utc)
    )
    lines = []
    for idx, (gid, gname, activated, member_count) in enumerate(chunk, start=start + 1):
        status = "Ativo" if activated else "Inativo"
        lines.append(f"**{idx}.** {gname} (`{gid}`) — {member_count} membros — {status}")
    embed.description = "\n".join(lines) or "Nenhum servidor."
    embed.set_footer(text=f"Pagina {page + 1}/{total_pages} | Total: {total} servidores")

    view = GuildPanelView(guilds_list, page, total_pages)
    return embed, view


class GuildPanelView(discord.ui.View):
    def __init__(self, guilds_list: list, page: int, total_pages: int):
        super().__init__(timeout=120)
        self.guilds_list = guilds_list
        self.page = page
        self.total_pages = total_pages

        start = page * GUILDS_PER_PAGE
        chunk = guilds_list[start:start + GUILDS_PER_PAGE]
        for slot, (gid, gname, activated, _) in enumerate(chunk):
            btn = discord.ui.Button(
                label=f"Remover: {gname[:20]}",
                style=discord.ButtonStyle.danger if activated else discord.ButtonStyle.secondary,
                custom_id=f"rem_{gid}",
                row=slot,
                disabled=(not activated),
            )
            self.add_item(btn)
            self._patch_button(btn, gid, gname)

        # Nav buttons on row 4
        if total_pages > 1:
            prev_btn = discord.ui.Button(label="◀ Anterior", style=discord.ButtonStyle.primary,
                                         custom_id="prev_page", row=4,
                                         disabled=(page == 0))
            prev_btn.callback = self._prev_page
            self.add_item(prev_btn)

            next_btn = discord.ui.Button(label="Próxima ▶", style=discord.ButtonStyle.primary,
                                         custom_id="next_page", row=4,
                                         disabled=(page >= total_pages - 1))
            next_btn.callback = self._next_page
            self.add_item(next_btn)

    def _patch_button(self, btn, guild_id, guild_name):
        async def _cb(interaction: discord.Interaction, gid=guild_id, gname=guild_name):
            if interaction.user.id != BOT_OWNER_ID:
                return await interaction.response.send_message("Apenas o dono do bot.", ephemeral=True)
            await interaction.response.defer()
            deactivate_guild(gid)
            guild = interaction.client.get_guild(gid)
            if guild:
                try:
                    notify_ch = next(
                        (ch for ch in guild.text_channels if ch.permissions_for(guild.me).send_messages), None
                    )
                    if notify_ch:
                        embed_notif = discord.Embed(
                            title="Acesso Removido",
                            description=(
                                "O acesso do bot neste servidor foi removido pelo dono do bot.\n"
                                "Para reativar, o dono do servidor deve usar o comando de ativacao novamente."
                            ),
                            color=0x000000,
                            timestamp=datetime.now(timezone.utc)
                        )
                        await notify_ch.send(embed=embed_notif)
                except Exception:
                    pass
            guilds_data = []
            for g in interaction.client.guilds:
                gd_info = get_guild_data(g.id)
                guilds_data.append((g.id, g.name, gd_info.get("activated", False), g.member_count))
            new_embed, new_view = build_guild_panel_embed_and_view(guilds_data, self.page)
            await interaction.message.edit(embed=new_embed, view=new_view)
            await interaction.followup.send(
                f"Permissao do servidor **{gname}** removida. O dono tera que refazer a ativacao.",
                ephemeral=True
            )
        btn.callback = _cb

    async def _prev_page(self, interaction: discord.Interaction):
        if interaction.user.id != BOT_OWNER_ID:
            return await interaction.response.send_message("Apenas o dono do bot.", ephemeral=True)
        await interaction.response.defer()
        guilds_data = []
        for g in interaction.client.guilds:
            gd_info = get_guild_data(g.id)
            guilds_data.append((g.id, g.name, gd_info.get("activated", False), g.member_count))
        new_embed, new_view = build_guild_panel_embed_and_view(guilds_data, self.page - 1)
        await interaction.message.edit(embed=new_embed, view=new_view)

    async def _next_page(self, interaction: discord.Interaction):
        if interaction.user.id != BOT_OWNER_ID:
            return await interaction.response.send_message("Apenas o dono do bot.", ephemeral=True)
        await interaction.response.defer()
        guilds_data = []
        for g in interaction.client.guilds:
            gd_info = get_guild_data(g.id)
            guilds_data.append((g.id, g.name, gd_info.get("activated", False), g.member_count))
        new_embed, new_view = build_guild_panel_embed_and_view(guilds_data, self.page + 1)
        await interaction.message.edit(embed=new_embed, view=new_view)


# ─── Anti-Spam ────────────────────────────────────────────────────────────────

spam_tracker = defaultdict(lambda: {"times": [], "warned": False})

async def _remove_mute_after(member, mute_role, delay=300):
    await asyncio.sleep(delay)
    try:
        await member.remove_roles(mute_role)
    except Exception:
        pass

async def handle_anti_spam(message):
    try:
        if not message.guild or not isinstance(message.author, discord.Member):
            return
        if not get_ar(message.guild.id, "anti_spam"):
            return
        if message.author.guild_permissions.administrator:
            return
        if not member_below_bot(message.guild, message.author):
            return

        key = f"{message.guild.id}-{message.author.id}"
        now = datetime.now(timezone.utc).timestamp()
        tracker = spam_tracker[key]
        tracker["times"] = [t for t in tracker["times"] if now - t < SPAM_WINDOW]
        tracker["times"].append(now)

        if len(tracker["times"]) >= SPAM_LIMIT:
            spam_tracker.pop(key, None)
            member = message.author

            mute_role = discord.utils.get(message.guild.roles, name="muted")
            action = "Aviso emitido"
            if mute_role and member_below_bot(message.guild, member):
                await member.add_roles(mute_role)
                action = "Mute aplicado (5 min)"
                asyncio.create_task(_remove_mute_after(member, mute_role, 300))

            embed = create_embed(message.guild, "Anti-Spam Ativado")
            embed.description = (
                f"**Usuario:** {member.mention} ({member.id})\n"
                f"**Acao:** {action}\n"
                f"**Canal:** {message.channel.name} ({message.channel.id})\n"
                f"**Motivo:** Excedeu {SPAM_LIMIT} mensagens em {SPAM_WINDOW}s\n"
                f"**Horario:** <t:{int(now)}:F>"
            )
            await send_log(message.guild, "anti_spam", embed)

            try:
                async for msg in message.channel.history(limit=20):
                    if msg.author.id == member.id:
                        await msg.delete()
            except Exception:
                pass
    except Exception as e:
        print(f"[ERRO] handle_anti_spam: {e}", flush=True)

# ─── Anti-Gore ────────────────────────────────────────────────────────────────

MEDIA_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp",
                    ".mp4", ".mov", ".avi", ".mkv", ".webm", ".wmv"}

async def handle_anti_gore(message):
    try:
        if not message.guild or not isinstance(message.author, discord.Member):
            return
        if not get_ar(message.guild.id, "anti_gore"):
            return
        if message.author.guild_permissions.administrator:
            return
        if not member_below_bot(message.guild, message.author):
            return

        has_media = False
        for attachment in message.attachments:
            ext = os.path.splitext(attachment.filename.lower())[1]
            if ext in MEDIA_EXTENSIONS:
                has_media = True
                break
        if not has_media:
            for embed in message.embeds:
                if embed.image or embed.video:
                    has_media = True
                    break

        if has_media:
            try:
                await message.delete()
            except Exception:
                pass
            now = int(datetime.now(timezone.utc).timestamp())
            embed = create_embed(message.guild, "Anti-Gore: Midia Bloqueada")
            embed.description = (
                f"**Usuario:** {message.author.mention} ({message.author.id})\n"
                f"**Canal:** {message.channel.mention}\n"
                f"**Arquivo(s):** {', '.join(a.filename for a in message.attachments) or 'Embed com midia'}\n"
                f"**Horario:** <t:{now}:F>"
            )
            await send_log(message.guild, "anti_gore", embed)
    except Exception as e:
        print(f"[ERRO] handle_anti_gore: {e}", flush=True)

# ─── Anti-Raid ────────────────────────────────────────────────────────────────

raid_tracker = defaultdict(lambda: defaultdict(lambda: {"times": []}))

async def track_raid_event(guild, event_type, audit_action=None):
    try:
        if not get_ar(guild.id, "anti_raid"):
            return
        now = datetime.now(timezone.utc).timestamp()
        tracker = raid_tracker[guild.id][event_type]
        tracker["times"] = [t for t in tracker["times"] if now - t < RAID_WINDOW]
        tracker["times"].append(now)
        current_count = len(tracker["times"])

        label = {
            "channel_create": "Criacao de canal",
            "channel_delete": "Exclusao de canal",
            "role_create":    "Criacao de cargo",
            "role_delete":    "Exclusao de cargo",
            "ban":            "Banimento",
        }.get(event_type, event_type)

        actor = None
        if audit_action:
            try:
                async for entry in guild.audit_logs(action=audit_action, limit=1):
                    actor = entry.user
                    break
            except Exception:
                pass

        embed_individual = create_embed(guild, f"Anti-Raid — {label} detectado")
        embed_individual.description = (
            f"**Acao:** {label}\n"
            f"**Responsavel:** {f'{actor} ({actor.id})' if actor else 'Desconhecido'}\n"
            f"**Contagem na janela:** {current_count}/{RAID_THRESHOLD} em {RAID_WINDOW}s\n"
            f"**Horario:** <t:{int(now)}:F>"
        )
        await send_log(guild, "anti_raid", embed_individual)

        if current_count >= RAID_THRESHOLD:
            raid_tracker[guild.id][event_type] = {"times": []}

            label_mass = {
                "channel_create": "Criacao em massa de canais",
                "channel_delete": "Exclusao em massa de canais",
                "role_create":    "Criacao em massa de cargos",
                "role_delete":    "Exclusao em massa de cargos",
                "ban":            "Banimentos em massa",
            }.get(event_type, event_type)

            if actor:
                actor_member = guild.get_member(actor.id)
                if actor_member and member_below_bot(guild, actor_member):
                    try:
                        await guild.ban(actor_member, reason=f"Anti-Raid: {label_mass}")
                        ban_note = f"Responsavel **banido automaticamente**."
                    except Exception:
                        ban_note = "Nao foi possivel banir automaticamente."
                else:
                    ban_note = "Responsavel tem cargo superior ou igual ao bot."
            else:
                ban_note = "Responsavel desconhecido."

            embed_alert = create_embed(guild, "ALERTA: Anti-Raid Ativado")
            embed_alert.description = (
                f"**Acao detectada:** {label_mass} ({RAID_THRESHOLD} em {RAID_WINDOW}s)\n"
                f"**Responsavel:** {f'{actor} ({actor.id})' if actor else 'Desconhecido'}\n"
                f"**Nota:** {ban_note}\n"
                f"**Horario:** <t:{int(now)}:F>"
            )
            embed_alert.add_field(name="Acao Recomendada", value="Verifique o servidor e remova permissoes suspeitas imediatamente.", inline=False)
            await send_log(guild, "anti_raid", embed_alert)

    except Exception as e:
        print(f"[ERRO] track_raid_event: {e}", flush=True)

# ─── Anti-Disconnect ──────────────────────────────────────────────────────────
# Novo comportamento: so atua quando um membro e forcado a sair de call por outro
# Quem TIROU o membro leva blacklist de cargo por 30 minutos

ANTI_DISCONNECT_DURATION = 30 * 60  # 30 minutos em segundos

async def _restore_roles_after(guild_id, member_id, role_ids, delay):
    """Restaura cargos apos o tempo de blacklist temporaria."""
    await asyncio.sleep(delay)
    try:
        guild = bot.get_guild(guild_id)
        if not guild:
            return
        member = guild.get_member(member_id)
        if not member:
            return
        for rid in role_ids:
            role = guild.get_role(int(rid))
            if role and role < guild.me.top_role:
                try:
                    await member.add_roles(role, reason="Restauracao Anti-Disconnect")
                except Exception:
                    pass
    except Exception as e:
        print(f"[ERRO] _restore_roles_after: {e}", flush=True)

async def handle_anti_disconnect(guild, member_who_left):
    """
    Detecta se a saida de call foi forcada por outro membro via audit log.
    Se sim, quem forcou perde todos os cargos por 30 minutos.
    """
    try:
        if not get_ar(guild.id, "anti_disconnect"):
            return

        now = datetime.now(timezone.utc)
        actor = None
        try:
            async for entry in guild.audit_logs(
                action=discord.AuditLogAction.member_disconnect, limit=3
            ):
                # Apenas entradas dos ultimos 3 segundos
                age = (now - entry.created_at.replace(tzinfo=timezone.utc)).total_seconds()
                if age <= 3:
                    actor = entry.user
                    break
        except Exception:
            return

        if not actor or actor.id == bot.user.id:
            return
        if actor.id == guild.owner_id:
            return

        actor_member = guild.get_member(actor.id)
        if not actor_member:
            return
        # Nao age contra quem e igual ou superior ao bot
        if not member_below_bot(guild, actor_member):
            return

        # Remove todos os cargos manipulaveis do ator por 30 minutos
        roles_to_remove = [
            r for r in actor_member.roles
            if not r.managed and r.id != guild.id and r < guild.me.top_role
        ]
        role_ids = [str(r.id) for r in roles_to_remove]

        for role in roles_to_remove:
            try:
                await actor_member.remove_roles(role, reason="Anti-Disconnect: desconectou membro a forca")
            except Exception:
                pass

        ts = int(now.timestamp())
        embed = create_embed(guild, "Anti-Disconnect: Blacklist de Cargo Aplicada")
        embed.description = (
            f"**Quem tirou:** {actor_member.mention} ({actor_member.id})\n"
            f"**Vitima:** {member_who_left.mention} ({member_who_left.id})\n"
            f"**Acao:** Todos os cargos removidos por 30 minutos\n"
            f"**Restauracao:** <t:{ts + ANTI_DISCONNECT_DURATION}:R>\n"
            f"**Horario:** <t:{ts}:F>"
        )
        await send_log(guild, "anti_disconnect", embed)

        # Agenda restauracao dos cargos
        asyncio.create_task(_restore_roles_after(guild.id, actor_member.id, role_ids, ANTI_DISCONNECT_DURATION))

    except Exception as e:
        print(f"[ERRO] handle_anti_disconnect: {e}", flush=True)

# ─── Blacklist ────────────────────────────────────────────────────────────────

TEMP_DURATIONS = [15 * 60, 30 * 60, 60 * 60, None]

def is_blacklisted(guild_id, user_id):
    try:
        gd = get_guild_data(guild_id)
        entry = gd["blacklist"].get(str(user_id))
        if not entry:
            return False
        if not entry["permanent"] and entry.get("expires_at"):
            if datetime.now(timezone.utc).timestamp() > entry["expires_at"]:
                del gd["blacklist"][str(user_id)]
                update_guild_data(guild_id, gd)
                return False
        return True
    except Exception:
        return False

def add_blacklist(guild_id, user_id, reason, added_by, permanent=True, duration=None):
    gd = get_guild_data(guild_id)
    current = gd["blacklist"].get(str(user_id), {})
    infractions = current.get("infractions", 0) + 1
    dur_index = min(infractions - 1, len(TEMP_DURATIONS) - 1)
    dur = None if permanent else (duration or TEMP_DURATIONS[dur_index])
    now = datetime.now(timezone.utc).timestamp()
    gd["blacklist"][str(user_id)] = {
        "user_id": str(user_id),
        "reason": reason,
        "permanent": dur is None,
        "expires_at": (now + dur) if dur else None,
        "infractions": infractions,
        "added_by": str(added_by),
        "added_at": now
    }
    update_guild_data(guild_id, gd)

def remove_blacklist(guild_id, user_id):
    gd = get_guild_data(guild_id)
    if str(user_id) not in gd["blacklist"]:
        return False
    del gd["blacklist"][str(user_id)]
    update_guild_data(guild_id, gd)
    return True

async def enforce_blacklist(member):
    try:
        if not is_blacklisted(member.guild.id, member.id):
            return
        for role in list(member.roles):
            if not role.managed and role.id != member.guild.id:
                try:
                    await member.remove_roles(role)
                except Exception:
                    pass
    except Exception as e:
        print(f"[ERRO] enforce_blacklist: {e}", flush=True)

# ─── Protected Users ──────────────────────────────────────────────────────────

def is_protected(guild_id, user_id):
    gd = get_guild_data(guild_id)
    return str(user_id) in gd.get("protected_users", [])

# ─── VIP System ───────────────────────────────────────────────────────────────

def is_vip(guild_id, user_id):
    return str(user_id) in get_guild_data(guild_id)["vip_users"]

def has_vip_perm(guild_id, user_id):
    return str(user_id) in get_guild_data(guild_id)["vip_perm_users"]

async def add_vip(guild, target_id, added_by):
    gd = get_guild_data(guild.id)
    if str(target_id) in gd["vip_users"]:
        return False
    role = None
    try:
        role = await guild.create_role(
            name=f"VIP - {str(target_id)[-4:]}",
            colour=discord.Colour(EMBED_COLOR),
            reason="Cargo VIP criado pelo bot YOV"
        )
    except Exception:
        pass
    gd["vip_users"][str(target_id)] = {
        "user_id": str(target_id),
        "role_id": str(role.id) if role else None,
        "anti_bans": [],
        "added_by": str(added_by),
        "added_at": datetime.now(timezone.utc).timestamp()
    }
    update_guild_data(guild.id, gd)
    if role:
        # Posiciona o cargo VIP logo abaixo do cargo de referencia (vip_above_role_id)
        above_role_id = gd.get("vip_above_role_id")
        if above_role_id:
            above_role = guild.get_role(int(above_role_id))
            if above_role:
                try:
                    # Posiciona o cargo VIP logo abaixo do cargo de referencia
                    new_position = max(1, above_role.position - 1)
                    await role.edit(position=new_position, reason="Posicionando cargo VIP abaixo do cargo de referencia")
                except Exception:
                    pass
        try:
            member = guild.get_member(target_id) or await guild.fetch_member(target_id)
            if member:
                await member.add_roles(role)
        except Exception:
            pass
    return True

async def remove_vip(guild, target_id):
    gd = get_guild_data(guild.id)
    vip = gd["vip_users"].get(str(target_id))
    if not vip:
        return False
    if vip.get("role_id"):
        role = guild.get_role(int(vip["role_id"]))
        if role:
            try:
                await role.delete(reason="VIP removido")
            except Exception:
                pass
    del gd["vip_users"][str(target_id)]
    update_guild_data(guild.id, gd)
    return True

def add_anti_ban(guild_id, target_id, added_by):
    gd = get_guild_data(guild_id)
    vip = gd["vip_users"].get(str(added_by))
    if not vip or len(vip["anti_bans"]) >= 5:
        return False
    if str(target_id) not in gd["anti_ban_users"]:
        gd["anti_ban_users"][str(target_id)] = []
    gd["anti_ban_users"][str(target_id)].append(str(added_by))
    gd["vip_users"][str(added_by)]["anti_bans"].append(str(target_id))
    update_guild_data(guild_id, gd)
    return True

def remove_anti_ban(guild_id, target_id, removed_by):
    gd = get_guild_data(guild_id)
    lst = gd["anti_ban_users"].get(str(target_id), [])
    if str(removed_by) not in lst:
        return False
    lst.remove(str(removed_by))
    gd["anti_ban_users"][str(target_id)] = lst
    if str(removed_by) in gd["vip_users"]:
        gd["vip_users"][str(removed_by)]["anti_bans"] = [
            x for x in gd["vip_users"][str(removed_by)]["anti_bans"] if x != str(target_id)
        ]
    update_guild_data(guild_id, gd)
    return True

def has_anti_ban(guild_id, user_id):
    gd = get_guild_data(guild_id)
    return len(gd["anti_ban_users"].get(str(user_id), [])) > 0

# ─── Ban Limits ───────────────────────────────────────────────────────────────

def check_ban_limit(guild_id, member):
    gd = get_guild_data(guild_id)
    now = datetime.now(timezone.utc).timestamp()
    for role in member.roles:
        limit_data = gd["ban_limits"].get(str(role.id))
        if not limit_data:
            continue
        if now > limit_data.get("reset_at", 0):
            limit_data["used"] = 0
            limit_data["reset_at"] = now + BAN_LIMIT_RESET_DAYS * 86400
        if limit_data["used"] >= limit_data["limit"]:
            update_guild_data(guild_id, gd)
            return False, limit_data["limit"]
        limit_data["used"] += 1
        update_guild_data(guild_id, gd)
        return True, None
    return True, None

# ─── Bot Setup ────────────────────────────────────────────────────────────────

async def get_prefix(bot, message):
    if message.guild:
        gd = get_guild_data(message.guild.id)
        return gd.get("prefix", PREFIX)
    return PREFIX

intents = discord.Intents.all()
bot = commands.Bot(command_prefix=get_prefix, intents=intents, help_command=None)

# ─── Painel do Dono do Bot ────────────────────────────────────────────────────

@bot.command(name="painel")
async def painel_cmd(ctx):
    """Painel de controle de servidores — apenas dono do bot, apenas na DM."""
    if ctx.author.id != BOT_OWNER_ID:
        return
    if ctx.guild:
        try:
            await ctx.message.delete()
        except Exception:
            pass
        try:
            await ctx.author.send(
                embed=discord.Embed(
                    title="Aviso",
                    description="O comando `painel` so pode ser usado na DM do bot.",
                    color=0x000000
                )
            )
        except Exception:
            pass
        return

    guilds_data = []
    for g in bot.guilds:
        gd_info = get_guild_data(g.id)
        guilds_data.append((g.id, g.name, gd_info.get("activated", False), g.member_count))

    guilds_data.sort(key=lambda x: x[1].lower())

    if not guilds_data:
        return await ctx.send(embed=discord.Embed(
            title="Painel de Servidores",
            description="O bot nao esta em nenhum servidor.",
            color=0x000000
        ))

    embed, view = build_guild_panel_embed_and_view(guilds_data, 0)
    await ctx.send(embed=embed, view=view)


# ─── Anti-Raid Panel View ────────────────────────────────────────────────────

def build_antiraid_embed(guild):
    gd = get_guild_data(guild.id)
    ar = gd.get("antiraid_settings", dict(DEFAULT_ANTIRAID))

    def status(key):
        return "Ativado" if ar.get(key, DEFAULT_ANTIRAID[key]) else "Desativado"

    embed = create_embed(guild, "Painel Anti-Raid")
    embed.description = (
        "Configure os modulos de protecao do servidor.\n"
        "Todas as protecoes so atuam em membros com cargo **inferior ao bot**.\n\n"
        f"**Anti-Spam:** {status('anti_spam')}\n"
        f"**Anti-Gore:** {status('anti_gore')}\n"
        f"**Anti-Raid:** {status('anti_raid')}\n"
        f"**Anti-Disconnect:** {status('anti_disconnect')}\n\n"
        f"Anti-Spam: bloqueia mensagens em rafaga ({SPAM_LIMIT} em {SPAM_WINDOW}s)\n"
        f"Anti-Gore: remove midias (imagens/videos) enviadas por membros inferiores\n"
        f"Anti-Raid: detecta acoes em massa e bane o responsavel automaticamente\n"
        f"Anti-Disconnect: {DISCONNECT_LIMIT} desconexoes de call em {DISCONNECT_WINDOW}s = blacklist automatica"
    )
    return embed

class AntiRaidPanelView(discord.ui.View):
    def __init__(self, owner_id, guild):
        super().__init__(timeout=120)
        self.owner_id = owner_id
        self.guild = guild
        self._update_buttons()

    def _update_buttons(self):
        gd = get_guild_data(self.guild.id)
        ar = gd.get("antiraid_settings", dict(DEFAULT_ANTIRAID))
        for child in self.children:
            if hasattr(child, "custom_id"):
                key = child.custom_id.replace("toggle_", "")
                is_on = ar.get(key, DEFAULT_ANTIRAID.get(key, False))
                child.style = discord.ButtonStyle.success if is_on else discord.ButtonStyle.danger

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "Apenas o dono do servidor pode usar este painel.", ephemeral=True)
            return False
        return True

    async def _toggle_and_update(self, interaction: discord.Interaction, key: str):
        await interaction.response.defer()
        toggle_ar(self.guild.id, key)
        self._update_buttons()
        await interaction.message.edit(
            embed=build_antiraid_embed(self.guild), view=self)

    @discord.ui.button(label="Anti-Spam", custom_id="toggle_anti_spam", style=discord.ButtonStyle.success, row=0)
    async def toggle_spam(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._toggle_and_update(interaction, "anti_spam")

    @discord.ui.button(label="Anti-Gore", custom_id="toggle_anti_gore", style=discord.ButtonStyle.danger, row=0)
    async def toggle_gore(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._toggle_and_update(interaction, "anti_gore")

    @discord.ui.button(label="Anti-Raid", custom_id="toggle_anti_raid", style=discord.ButtonStyle.success, row=1)
    async def toggle_raid(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._toggle_and_update(interaction, "anti_raid")

    @discord.ui.button(label="Anti-Disconnect", custom_id="toggle_anti_disconnect", style=discord.ButtonStyle.success, row=1)
    async def toggle_disconnect(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._toggle_and_update(interaction, "anti_disconnect")

# ─── Events ───────────────────────────────────────────────────────────────────

@bot.event
async def on_ready():
    print(f"[BOT] Online como {bot.user}", flush=True)
    print(f"[BOT] Servidores: {len(bot.guilds)}", flush=True)

    # ── Sync de servidores no banco ──────────────────────────────────────────
    # Garante que todos os servidores em que o bot esta tenham uma linha no banco.
    # Nao sobrescreve o campo "activated" de servidores ja registrados.
    # Isso evita que redeploys no Railway forcam re-ativacao.
    if DATABASE_URL and _PSYCOPG2_AVAILABLE:
        try:
            conn = _pg_connect()
            cur = conn.cursor()
            for guild in bot.guilds:
                gid = str(guild.id)
                cur.execute("SELECT guild_id FROM guild_data WHERE guild_id = %s", (gid,))
                if cur.fetchone() is None:
                    # Servidor novo no banco — cria com dados padrao (activated=False)
                    default = _make_default_guild_data()
                    cur.execute("""
                        INSERT INTO guild_data (guild_id, guild_name, data)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (guild_id) DO NOTHING
                    """, (gid, guild.name, json.dumps(default, ensure_ascii=False)))
                    print(f"[DB] Servidor novo registrado: {guild.name} ({gid})", flush=True)
                else:
                    # Servidor ja existe — apenas atualiza o nome (nao toca no activated)
                    cur.execute("""
                        UPDATE guild_data SET guild_name = %s, updated_at = NOW()
                        WHERE guild_id = %s
                    """, (guild.name, gid))
            conn.commit()
            cur.close()
            conn.close()
            print(f"[DB] Sync concluido: {len(bot.guilds)} servidor(es) verificado(s).", flush=True)
        except Exception as e:
            print(f"[ERRO] on_ready sync: {e}", flush=True)

    try:
        await bot.change_presence(activity=discord.Game(name="yov!help | Anti-Raid ativo"))
    except Exception:
        pass

@bot.event
async def on_error(event, *args, **kwargs):
    print(f"[ANTI-CRASH] Erro no evento '{event}':", flush=True)
    traceback.print_exc()

@bot.event
async def on_disconnect():
    print("[BOT] Desconectado. Reconectando...", flush=True)

@bot.event
async def on_resumed():
    print("[BOT] Reconectado com sucesso.", flush=True)

@bot.event
async def on_guild_join(guild):
    """Ao entrar em um novo servidor, registra no banco e notifica o dono do bot para aprovacao."""
    try:
        print(f"[BOT] Entrou no servidor: {guild.name} ({guild.id})", flush=True)
        # Cria / atualiza a entrada do servidor no banco de dados
        pg_register_guild(guild.id, guild.name)
        # Garante que o registro padrao existe (cria se novo)
        get_guild_data(guild.id)
        if BOT_OWNER_ID == 0:
            print("[AVISO] BOT_OWNER_ID nao configurado. Ativacao automatica desabilitada.", flush=True)
            return

        owner = await bot.fetch_user(BOT_OWNER_ID)
        if not owner:
            return

        requestor = guild.owner
        embed = discord.Embed(
            title="Novo Servidor — Aprovacao Necessaria",
            color=0x000000,
            timestamp=datetime.now(timezone.utc)
        )
        embed.add_field(name="Servidor", value=f"{guild.name}", inline=True)
        embed.add_field(name="ID", value=str(guild.id), inline=True)
        embed.add_field(name="Membros", value=str(guild.member_count), inline=True)
        embed.add_field(name="Dono do servidor", value=f"{requestor} ({requestor.id})" if requestor else "Desconhecido", inline=False)
        embed.set_footer(text="Clique Aprovar para ativar o bot ou Negar para removê-lo do servidor.")
        if guild.icon:
            embed.set_thumbnail(url=guild.icon.url)

        view = ApprovalView(
            guild_id=guild.id,
            guild_name=guild.name,
            requestor_name=str(requestor) if requestor else "Desconhecido",
            requestor_id=requestor.id if requestor else 0
        )
        await owner.send(embed=embed, view=view)

        # Notifica no servidor que aprovacao esta pendente
        notify_ch = next((ch for ch in guild.text_channels if ch.permissions_for(guild.me).send_messages), None)
        if notify_ch:
            p = get_guild_data(guild.id).get("prefix", PREFIX)
            pend_embed = discord.Embed(
                title="Bot Adicionado — Aguardando Aprovacao",
                description=(
                    f"O bot foi adicionado ao servidor!\n\n"
                    f"Para comecar a usar, o dono do servidor deve ativar com a chave:\n"
                    f"**`{p}ativar <chave>`**\n\n"
                    f"Solicite a chave ao dono do bot."
                ),
                color=0x7c3aed,
                timestamp=datetime.now(timezone.utc)
            )
            await notify_ch.send(embed=pend_embed)
    except Exception as e:
        print(f"[ERRO] on_guild_join: {e}", flush=True)

def _extrair_guild_id_e_nome(message):
    """
    Extrai guild_id (17-19 digitos) e nome do servidor da mensagem,
    verificando tanto message.content quanto os embeds (description e fields).
    Retorna (guild_id: int, guild_nome: str | None).
    """
    import re

    # Junta todo o texto disponivel na mensagem (content + embeds)
    partes = [message.content or ""]
    for emb in message.embeds:
        if emb.description:
            partes.append(emb.description)
        for field in emb.fields:
            partes.append(field.name or "")
            partes.append(field.value or "")
        if emb.footer and emb.footer.text:
            partes.append(emb.footer.text)
        if emb.title:
            partes.append(emb.title)
    texto_total = " ".join(partes)

    match = re.search(r"\b(\d{17,19})\b", texto_total)
    if not match:
        return None, None

    guild_id = int(match.group(1))
    # O nome e o texto sem o ID, limpo de caracteres especiais
    resto = texto_total[:match.start()].strip() + " " + texto_total[match.end():].strip()
    nome = re.sub(r"[^\w\s\-]", " ", resto).strip()
    return guild_id, nome or None


async def _handle_deactivate_request(message):
    """
    Processa um pedido de desativacao vindo do bot autorizado (DEACTIVATE_BOT_ID).
    1. Encaminha a mensagem original ao dono do bot via DM.
    2. Desativa o servidor detectado.
    3. Avisa o dono do servidor para reativar a key e abrir suporte.
    """
    # 1. Encaminha a mensagem original ao dono do bot
    try:
        bot_owner = await bot.fetch_user(BOT_OWNER_ID)
        if bot_owner:
            embed_aviso = discord.Embed(
                title="⚠️ Pedido de Desativacao Recebido",
                color=0x000000,
                timestamp=datetime.now(timezone.utc),
            )
            embed_aviso.add_field(name="Conteudo da mensagem", value=message.content or "(sem texto)", inline=False)
            # Repassa o primeiro embed recebido, se houver
            if message.embeds:
                orig = message.embeds[0]
                embed_aviso.add_field(
                    name="Embed original",
                    value=(
                        f"**Titulo:** {orig.title or '—'}\n"
                        f"**Descricao:** {orig.description or '—'}"
                    ),
                    inline=False,
                )
            embed_aviso.set_footer(text=f"Enviado pelo bot {DEACTIVATE_BOT_ID}")
            await bot_owner.send(embed=embed_aviso)
    except Exception as e:
        print(f"[DESATIVAR] Falha ao encaminhar mensagem ao BOT_OWNER_ID: {e}", flush=True)

    # 2. Extrai o ID do servidor e desativa
    guild_id, guild_nome = _extrair_guild_id_e_nome(message)
    if not guild_id:
        print(f"[DESATIVAR] Nenhum guild_id encontrado na mensagem do bot {DEACTIVATE_BOT_ID}.", flush=True)
        try:
            if bot_owner:
                await bot_owner.send(
                    embed=discord.Embed(
                        title="Erro — ID Nao Encontrado",
                        description="A mensagem de desativacao foi recebida mas nenhum ID de servidor (17-19 digitos) foi encontrado.",
                        color=0xff0000,
                        timestamp=datetime.now(timezone.utc),
                    )
                )
        except Exception:
            pass
        return

    guild_nome = guild_nome or "Desconhecido"
    deactivate_guild(guild_id)
    print(f"[DESATIVAR] Servidor {guild_nome} ({guild_id}) desativado.", flush=True)

    guild = bot.get_guild(guild_id)
    nome_real = guild.name if guild else guild_nome

    # 3. DM ao dono do servidor
    owner_dm_sent = False
    if guild:
        try:
            owner = guild.owner or await bot.fetch_user(guild.owner_id)
            if owner:
                embed_owner = discord.Embed(
                    title="⚠️ Bot Desativado no Seu Servidor",
                    description=(
                        f"O bot foi **desativado** no servidor **{nome_real}**.\n\n"
                        f"Para voltar a usar o bot voce precisa:\n"
                        f"**1.** Reativar usando a chave de ativacao (`yov!ativar <chave>`)\n"
                        f"**2.** Solicitar acesso abrindo um ticket no nosso servidor oficial"
                    ),
                    color=0x000000,
                    timestamp=datetime.now(timezone.utc),
                )
                embed_owner.add_field(
                    name="Servidor de Suporte",
                    value=f"[Clique aqui para abrir suporte]({SUPPORT_SERVER})\n{SUPPORT_SERVER}",
                    inline=False,
                )
                embed_owner.set_footer(text="Entre no servidor e abra um ticket para reativar")
                await owner.send(embed=embed_owner)
                owner_dm_sent = True
        except Exception as e:
            print(f"[DESATIVAR] Falha ao DM dono do servidor {guild_id}: {e}", flush=True)

    # 4. Confirma ao dono do bot que tudo foi feito
    try:
        if bot_owner:
            embed_confirmacao = discord.Embed(
                title="Servidor Desativado com Sucesso",
                color=0x000000,
                timestamp=datetime.now(timezone.utc),
            )
            embed_confirmacao.add_field(name="Servidor", value=f"{nome_real}", inline=True)
            embed_confirmacao.add_field(name="ID", value=f"`{guild_id}`", inline=True)
            embed_confirmacao.add_field(
                name="DM ao dono do servidor",
                value="Enviada ✅" if owner_dm_sent else "Falhou ❌ (guild nao encontrada ou DMs fechadas)",
                inline=False,
            )
            await bot_owner.send(embed=embed_confirmacao)
    except Exception as e:
        print(f"[DESATIVAR] Falha ao confirmar ao BOT_OWNER_ID: {e}", flush=True)


@bot.event
async def on_message(message):
    try:
        # ── Pedido de desativacao do bot autorizado ──
        if message.author.id == DEACTIVATE_BOT_ID:
            await _handle_deactivate_request(message)
            return

        if message.author.bot:
            return
        # Permitir comandos na DM (ex: painel)
        if not message.guild:
            await bot.process_commands(message)
            return
        await handle_anti_spam(message)
        await handle_anti_gore(message)
        if isinstance(message.author, discord.Member) and is_blacklisted(message.guild.id, message.author.id):
            await enforce_blacklist(message.author)
        # ── Gatilhos CL ──
        gd_msg = get_guild_data(message.guild.id)
        if gd_msg.get("activated", False):
            pfx = gd_msg.get("prefix", PREFIX)
            cl_gatilhos = [pw.lower() for pw in gd_msg.get("cl_palavras", [])]
            content_lower = message.content.strip().lower()
            for gatilho in cl_gatilhos:
                if content_lower in (gatilho, pfx + gatilho):
                    if isinstance(message.author, discord.Member) and _tem_permissao_cl(message.author, gd_msg):
                        await _executar_cl(message, gd_msg)
                    return
        await bot.process_commands(message)
    except Exception as e:
        print(f"[ERRO] on_message: {e}", flush=True)

@bot.event
async def on_member_join(member):
    try:
        if is_blacklisted(member.guild.id, member.id):
            await enforce_blacklist(member)
        if member.bot:
            embed = create_embed(member.guild, "Bot Entrou no Servidor")
            embed.description = (
                f"**Bot:** {member}\n"
                f"**ID:** {member.id}\n"
                f"**Horario:** <t:{int(datetime.now(timezone.utc).timestamp())}:F>"
            )
            await send_log(member.guild, "bot_join", embed)
            return
        embed = create_embed(member.guild, "Membro Entrou")
        embed.description = (
            f"**Usuario:** {member.mention} ({member.id})\n"
            f"**Conta criada:** <t:{int(member.created_at.timestamp())}:R>\n"
            f"**Horario:** <t:{int(datetime.now(timezone.utc).timestamp())}:F>"
        )
        await send_log(member.guild, "members", embed)

        # ── Boas-vindas automáticas ──
        try:
            gd = get_guild_data(member.guild.id)
            wc = gd.get("welcome_config", {})
            if wc.get("enabled") and wc.get("channel_id"):
                ch = member.guild.get_channel(int(wc["channel_id"]))
                if ch:
                    count = member.guild.member_count
                    title = wc.get("title", "Bem-vindo(a) ao servidor!")
                    desc  = wc.get("description", "Olá, {user}!")
                    desc  = desc.replace("{user}", member.mention)
                    desc  = desc.replace("{server}", member.guild.name)
                    desc  = desc.replace("{count}", str(count))
                    title = title.replace("{user}", str(member))
                    title = title.replace("{server}", member.guild.name)
                    title = title.replace("{count}", str(count))
                    wc_color = wc.get("color")
                    if wc_color:
                        try:
                            wembed = discord.Embed(
                                color=int(wc_color.lstrip("#"), 16),
                                timestamp=datetime.now(timezone.utc)
                            )
                            wembed.title = title
                        except (ValueError, AttributeError):
                            wembed = create_embed(member.guild, title)
                    else:
                        wembed = create_embed(member.guild, title)
                    wembed.description = desc
                    wembed.set_thumbnail(url=member.display_avatar.url)
                    msg = await ch.send(embed=wembed)
                    await asyncio.sleep(10)
                    try:
                        await msg.delete()
                    except Exception:
                        pass
        except Exception as e:
            print(f"[ERRO] welcome: {e}", flush=True)

    except Exception as e:
        print(f"[ERRO] on_member_join: {e}", flush=True)

@bot.event
async def on_member_remove(member):
    try:
        if member.bot:
            return
        for action in (discord.AuditLogAction.kick, discord.AuditLogAction.ban):
            try:
                async for entry in member.guild.audit_logs(action=action, limit=1):
                    if entry.target.id == member.id:
                        return
            except Exception:
                pass
        embed = create_embed(member.guild, "Membro Saiu")
        embed.description = (
            f"**Usuario:** {member.mention} ({member.id})\n"
            f"**Horario:** <t:{int(datetime.now(timezone.utc).timestamp())}:F>"
        )
        await send_log(member.guild, "members", embed)
    except Exception as e:
        print(f"[ERRO] on_member_remove: {e}", flush=True)

@bot.event
async def on_member_update(before, after):
    try:
        if is_blacklisted(after.guild.id, after.id):
            await enforce_blacklist(after)

        now = int(datetime.now(timezone.utc).timestamp())

        if before.timed_out_until != after.timed_out_until:
            mute_key = (after.guild.id, after.id, "mute")
            if mute_key in _pending_bot_actions:
                _pending_bot_actions.discard(mute_key)
            elif after.timed_out_until:
                mute_actor = None
                try:
                    async for entry in after.guild.audit_logs(action=discord.AuditLogAction.member_update, limit=1):
                        if entry.target.id == after.id:
                            mute_actor = entry.user
                            break
                except Exception:
                    pass
                embed = create_embed(after.guild, "Membro Mutado")
                embed.description = (
                    f"**Usuario:** {after.mention} ({after.id})\n"
                    f"**Moderador:** {f'{mute_actor.mention} ({mute_actor.id})' if mute_actor else 'Desconhecido'}\n"
                    f"**Ate:** <t:{int(after.timed_out_until.timestamp())}:F>\n"
                    f"**Horario:** <t:{now}:F>"
                )
                await send_log(after.guild, "mute", embed)
            else:
                unmute_actor = None
                try:
                    async for entry in after.guild.audit_logs(action=discord.AuditLogAction.member_update, limit=1):
                        if entry.target.id == after.id:
                            unmute_actor = entry.user
                            break
                except Exception:
                    pass
                embed = create_embed(after.guild, "Mute Removido")
                embed.description = (
                    f"**Usuario:** {after.mention} ({after.id})\n"
                    f"**Removido por:** {f'{unmute_actor.mention} ({unmute_actor.id})' if unmute_actor else 'Desconhecido'}\n"
                    f"**Horario:** <t:{now}:F>"
                )
                await send_log(after.guild, "mute", embed)

        castigo_role = discord.utils.get(after.guild.roles, name="castigo")
        if castigo_role:
            before_has = castigo_role in before.roles
            after_has = castigo_role in after.roles
            if before_has != after_has:
                castigo_key = (after.guild.id, after.id, "castigo")
                if castigo_key in _pending_bot_actions:
                    _pending_bot_actions.discard(castigo_key)
                else:
                    role_actor = None
                    try:
                        async for entry in after.guild.audit_logs(action=discord.AuditLogAction.member_role_update, limit=1):
                            if entry.target.id == after.id:
                                role_actor = entry.user
                                break
                    except Exception:
                        pass
                    if after_has:
                        actor_member = after.guild.get_member(role_actor.id) if role_actor else None
                        bot_top_role = after.guild.me.top_role
                        # Bloqueia castigo manual de quem esta abaixo do bot
                        if actor_member and actor_member.top_role < bot_top_role and actor_member.id != after.guild.owner_id:
                            try:
                                _pending_bot_actions.add((after.guild.id, after.id, "castigo"))
                                await after.remove_roles(castigo_role, reason="Cargo insuficiente para aplicar castigo manualmente")
                            except Exception:
                                _pending_bot_actions.discard((after.guild.id, after.id, "castigo"))
                            embed = create_embed(after.guild, "Castigo Negado")
                            embed.description = (
                                f"**Tentativa por:** {actor_member.mention} ({actor_member.id})\n"
                                f"**Usuario:** {after.mention} ({after.id})\n"
                                f"**Motivo:** Cargo abaixo do bot — sem permissao\n"
                                f"**Horario:** <t:{now}:F>"
                            )
                            await send_log(after.guild, "castigo", embed)
                        else:
                            embed = create_embed(after.guild, "Membro em Castigo")
                            embed.description = (
                                f"**Usuario:** {after.mention} ({after.id})\n"
                                f"**Moderador:** {f'{role_actor.mention} ({role_actor.id})' if role_actor else 'Desconhecido'}\n"
                                f"**Horario:** <t:{now}:F>"
                            )
                            await send_log(after.guild, "castigo", embed)
                    else:
                        embed = create_embed(after.guild, "Castigo Removido")
                        embed.description = (
                            f"**Usuario:** {after.mention} ({after.id})\n"
                            f"**Removido por:** {f'{role_actor.mention} ({role_actor.id})' if role_actor else 'Desconhecido'}\n"
                            f"**Horario:** <t:{now}:F>"
                        )
                        await send_log(after.guild, "castigo", embed)

    except Exception as e:
        print(f"[ERRO] on_member_update: {e}", flush=True)

@bot.event
async def on_member_ban(guild, user):
    try:
        await track_raid_event(guild, "ban", discord.AuditLogAction.ban)
        actor = None
        try:
            async for entry in guild.audit_logs(action=discord.AuditLogAction.ban, limit=1):
                if entry.target.id == user.id:
                    actor = entry.user
                    break
        except Exception:
            pass
        if not actor or actor.id == bot.user.id:
            return
        now = int(datetime.now(timezone.utc).timestamp())
        actor_member = guild.get_member(actor.id)
        bot_top_role = guild.me.top_role

        # Bloqueia ban manual de qualquer um com cargo abaixo do bot
        if actor_member and actor_member.top_role < bot_top_role and actor_member.id != guild.owner_id:
            try:
                await guild.unban(user, reason="Cargo insuficiente para banir manualmente")
            except Exception:
                pass
            embed = create_embed(guild, "Banimento Negado")
            embed.description = (
                f"**Tentativa por:** {actor_member.mention} ({actor_member.id})\n"
                f"**Usuario:** {user.mention} ({user.id})\n"
                f"**Motivo:** Cargo abaixo do bot — sem permissao\n"
                f"**Horario:** <t:{now}:F>"
            )
            await send_log(guild, "ban", embed)
        else:
            embed = create_embed(guild, "Membro Banido (Manual)")
            embed.description = (
                f"**Usuario:** {user.mention} ({user.id})\n"
                f"**Banido por:** {f'{actor.mention} ({actor.id})' if actor else 'Desconhecido'}\n"
                f"**Horario:** <t:{now}:F>"
            )
            await send_log(guild, "ban", embed)
    except Exception as e:
        print(f"[ERRO] on_member_ban: {e}", flush=True)

@bot.event
async def on_guild_channel_create(channel):
    try:
        if not channel.guild:
            return
        actor = None
        try:
            async for entry in channel.guild.audit_logs(action=discord.AuditLogAction.channel_create, limit=1):
                actor = entry.user
                break
        except Exception:
            pass
        if actor and actor.id == bot.user.id:
            return
        await track_raid_event(channel.guild, "channel_create", discord.AuditLogAction.channel_create)
        embed = create_embed(channel.guild, "Canal Criado")
        embed.description = (
            f"**Canal:** {channel.name} ({channel.id})\n"
            f"**Responsavel:** {f'{actor.mention} ({actor.id})' if actor else 'Desconhecido'}\n"
            f"**Horario:** <t:{int(datetime.now(timezone.utc).timestamp())}:F>"
        )
        await send_log(channel.guild, "channel_create", embed)
    except Exception as e:
        print(f"[ERRO] on_guild_channel_create: {e}", flush=True)

@bot.event
async def on_guild_channel_delete(channel):
    try:
        if not channel.guild:
            return
        actor = None
        try:
            async for entry in channel.guild.audit_logs(action=discord.AuditLogAction.channel_delete, limit=1):
                actor = entry.user
                break
        except Exception:
            pass
        if actor and actor.id == bot.user.id:
            return
        await track_raid_event(channel.guild, "channel_delete", discord.AuditLogAction.channel_delete)
        embed = create_embed(channel.guild, "Canal Excluido")
        embed.description = (
            f"**Canal:** {channel.name} ({channel.id})\n"
            f"**Responsavel:** {f'{actor.mention} ({actor.id})' if actor else 'Desconhecido'}\n"
            f"**Horario:** <t:{int(datetime.now(timezone.utc).timestamp())}:F>"
        )
        await send_log(channel.guild, "channel_delete", embed)
    except Exception as e:
        print(f"[ERRO] on_guild_channel_delete: {e}", flush=True)

@bot.event
async def on_guild_role_create(role):
    try:
        actor = None
        try:
            async for entry in role.guild.audit_logs(action=discord.AuditLogAction.role_create, limit=1):
                actor = entry.user
                break
        except Exception:
            pass
        if actor and actor.id == bot.user.id:
            return
        await track_raid_event(role.guild, "role_create", discord.AuditLogAction.role_create)
        embed = create_embed(role.guild, "Cargo Criado")
        embed.description = (
            f"**Cargo:** {role.name} ({role.id})\n"
            f"**Cor:** {str(role.colour)}\n"
            f"**Responsavel:** {f'{actor.mention} ({actor.id})' if actor else 'Desconhecido'}\n"
            f"**Horario:** <t:{int(datetime.now(timezone.utc).timestamp())}:F>"
        )
        await send_log(role.guild, "role_create", embed)
    except Exception as e:
        print(f"[ERRO] on_guild_role_create: {e}", flush=True)

@bot.event
async def on_guild_role_delete(role):
    try:
        actor = None
        try:
            async for entry in role.guild.audit_logs(action=discord.AuditLogAction.role_delete, limit=1):
                actor = entry.user
                break
        except Exception:
            pass
        if actor and actor.id == bot.user.id:
            return
        await track_raid_event(role.guild, "role_delete", discord.AuditLogAction.role_delete)
        embed = create_embed(role.guild, "Cargo Excluido")
        embed.description = (
            f"**Cargo:** {role.name} ({role.id})\n"
            f"**Responsavel:** {f'{actor.mention} ({actor.id})' if actor else 'Desconhecido'}\n"
            f"**Horario:** <t:{int(datetime.now(timezone.utc).timestamp())}:F>"
        )
        await send_log(role.guild, "role_delete", embed)
    except Exception as e:
        print(f"[ERRO] on_guild_role_delete: {e}", flush=True)

@bot.event
async def on_voice_state_update(member, before, after):
    try:
        now = int(datetime.now(timezone.utc).timestamp())

        if not before.channel and after.channel:
            embed = create_embed(member.guild, "Entrou em Call")
            embed.description = (
                f"**Usuario:** {member.mention} ({member.id})\n"
                f"**Canal:** {after.channel.name} ({after.channel.id})\n"
                f"**Horario:** <t:{now}:F>"
            )
            await send_log(member.guild, "voice_join", embed)

        elif before.channel and not after.channel:
            embed = create_embed(member.guild, "Saiu de Call")
            embed.description = (
                f"**Usuario:** {member.mention} ({member.id})\n"
                f"**Canal:** {before.channel.name} ({before.channel.id})\n"
                f"**Horario:** <t:{now}:F>"
            )
            await send_log(member.guild, "voice_leave", embed)
            await handle_anti_disconnect(member.guild, member)

        if before.mute != after.mute:
            mute_actor = None
            try:
                async for entry in member.guild.audit_logs(action=discord.AuditLogAction.member_update, limit=1):
                    if entry.target.id == member.id:
                        mute_actor = entry.user
                        break
            except Exception:
                pass
            if after.mute:
                embed = create_embed(member.guild, "Mute de Call Aplicado")
                embed.description = (
                    f"**Usuario:** {member.mention} ({member.id})\n"
                    f"**Mutado por:** {f'{mute_actor.mention} ({mute_actor.id})' if mute_actor else 'Desconhecido'}\n"
                    f"**Canal:** {after.channel.name if after.channel else 'Desconhecido'}\n"
                    f"**Horario:** <t:{now}:F>"
                )
            else:
                embed = create_embed(member.guild, "Mute de Call Removido")
                embed.description = (
                    f"**Usuario:** {member.mention} ({member.id})\n"
                    f"**Desmutado por:** {f'{mute_actor.mention} ({mute_actor.id})' if mute_actor else 'Desconhecido'}\n"
                    f"**Canal:** {after.channel.name if after.channel else 'Desconhecido'}\n"
                    f"**Horario:** <t:{now}:F>"
                )
            await send_log(member.guild, "voice_mute", embed)

    except Exception as e:
        print(f"[ERRO] on_voice_state_update: {e}", flush=True)

@bot.event
async def on_guild_update(before, after):
    try:
        changes = []
        if before.name != after.name:
            changes.append(f"**Nome:** `{before.name}` -> `{after.name}`")
        if before.description != after.description:
            changes.append("**Descricao:** Alterada")
        if before.icon != after.icon:
            changes.append("**Icone:** Alterado")
        if before.banner != after.banner:
            changes.append("**Banner:** Alterado")

        if before.vanity_url_code != after.vanity_url_code:
            gd = get_guild_data(after.id)
            url_actor = None
            try:
                async for entry in after.audit_logs(action=discord.AuditLogAction.guild_update, limit=1):
                    url_actor = entry.user
                    break
            except Exception:
                pass
            embed = create_embed(after, "URL Personalizada Alterada")
            embed.description = (
                f"**URL anterior:** {before.vanity_url_code or 'Nenhuma'}\n"
                f"**Nova URL:** {after.vanity_url_code or 'Removida'}\n"
                f"**Responsavel:** {f'{url_actor.mention} ({url_actor.id})' if url_actor else 'Desconhecido'}\n"
                f"**Horario:** <t:{int(datetime.now(timezone.utc).timestamp())}:F>"
            )
            await send_log(after, "url_update", embed)
            if gd["vanity_url"] and after.vanity_url_code != gd["vanity_url"]:
                try:
                    await after.edit(vanity_code=gd["vanity_url"])
                except Exception:
                    pass
            elif not gd["vanity_url"] and after.vanity_url_code:
                gd["vanity_url"] = after.vanity_url_code
                update_guild_data(after.id, gd)

        if not changes:
            return
        srv_actor = None
        try:
            async for entry in after.audit_logs(action=discord.AuditLogAction.guild_update, limit=1):
                srv_actor = entry.user
                break
        except Exception:
            pass
        embed = create_embed(after, "Servidor Alterado")
        embed.description = (
            "\n".join(changes) +
            f"\n**Responsavel:** {f'{srv_actor.mention} ({srv_actor.id})' if srv_actor else 'Desconhecido'}\n"
            f"**Horario:** <t:{int(datetime.now(timezone.utc).timestamp())}:F>"
        )
        await send_log(after, "server_update", embed)
    except Exception as e:
        print(f"[ERRO] on_guild_update: {e}", flush=True)

@bot.event
async def on_message_edit(before, after):
    try:
        if not after.guild or after.author.bot:
            return
        if before.content == after.content:
            return
        now = int(datetime.now(timezone.utc).timestamp())
        embed = create_embed(after.guild, "Mensagem Editada")
        embed.description = (
            f"**Autor:** {after.author.mention} ({after.author.id})\n"
            f"**Canal:** {after.channel.mention} ({after.channel.id})\n"
            f"**Antes:** {before.content[:1000] or '*vazio*'}\n"
            f"**Depois:** {after.content[:1000] or '*vazio*'}\n"
            f"**Link:** [Ir para mensagem]({after.jump_url})\n"
            f"**Horario:** <t:{now}:F>"
        )
        await send_log(after.guild, "message_edit", embed)
    except Exception as e:
        print(f"[ERRO] on_message_edit: {e}", flush=True)

@bot.event
async def on_message_delete(message):
    try:
        if not message.guild or message.author.bot:
            return
        now = int(datetime.now(timezone.utc).timestamp())
        actor = None
        try:
            async for entry in message.guild.audit_logs(action=discord.AuditLogAction.message_delete, limit=1):
                if entry.target.id == message.author.id:
                    actor = entry.user
                    break
        except Exception:
            pass
        embed = create_embed(message.guild, "Mensagem Deletada")
        embed.description = (
            f"**Autor:** {message.author.mention} ({message.author.id})\n"
            f"**Canal:** {message.channel.mention} ({message.channel.id})\n"
            f"**Conteudo:** {message.content[:1000] or '*vazio*'}\n"
            f"**Deletado por:** {f'{actor.mention} ({actor.id})' if actor else 'Proprio autor ou desconhecido'}\n"
            f"**Horario:** <t:{now}:F>"
        )
        await send_log(message.guild, "message_delete", embed)
    except Exception as e:
        print(f"[ERRO] on_message_delete: {e}", flush=True)

@bot.event
async def on_command(ctx):
    try:
        if not ctx.guild:
            return
        p = await get_prefix(bot, ctx.message)
        if isinstance(p, list):
            p = p[0]
        embed = create_embed(ctx.guild, "Comando Executado")
        args_str = " ".join(str(a) for a in ctx.args[1:]) or "Nenhum"
        embed.description = (
            f"**Comando:** {p}{ctx.command.name}\n"
            f"**Usuario:** {ctx.author.mention} ({ctx.author.id})\n"
            f"**Canal:** {ctx.channel.mention}\n"
            f"**Argumentos:** {args_str}\n"
            f"**Horario:** <t:{int(datetime.now(timezone.utc).timestamp())}:F>"
        )
        await send_log(ctx.guild, "commands", embed)
    except Exception as e:
        print(f"[ERRO] on_command: {e}", flush=True)

# ─── Helper ───────────────────────────────────────────────────────────────────

async def delete_after(msg, seconds=DELETE_TIMEOUT):
    await asyncio.sleep(seconds)
    try:
        await msg.delete()
    except Exception:
        pass

async def reply_and_delete(ctx, embed):
    try:
        msg = await ctx.reply(embed=embed)
        asyncio.create_task(delete_after(msg))
        try:
            await ctx.message.delete()
        except Exception:
            pass
    except Exception as e:
        print(f"[ERRO] reply_and_delete: {e}", flush=True)

# ─── Ativar Servidor ──────────────────────────────────────────────────────────

@bot.command(name="ativar")
async def ativar(ctx, chave: str = None):
    """Ativa o bot no servidor usando a chave secreta. Apenas o dono do servidor."""
    if not ctx.guild:
        return
    if ctx.author.id != ctx.guild.owner_id:
        return await reply_and_delete(ctx, error_embed(
            "Apenas o dono do servidor pode ativar o bot.", ctx.guild))
    if not chave:
        return await reply_and_delete(ctx, error_embed(
            "Voce precisa informar a chave. Use: `ativar <chave>`", ctx.guild))
    if chave != _ACTIVATION_KEY:
        return await reply_and_delete(ctx, error_embed(
            "Chave invalida. Solicite a chave correta ao dono do bot.", ctx.guild))

    gd = get_guild_data(ctx.guild.id)
    if gd.get("activated", False):
        return await reply_and_delete(ctx, success_embed(
            "Ja Ativado", "Este servidor ja esta ativado.", ctx.guild))

    try:
        await ctx.message.delete()
    except Exception:
        pass

    if BOT_OWNER_ID == 0:
        activate_guild(ctx.guild.id)
        await ctx.channel.send(embed=success_embed(
            "Bot Ativado",
            "O servidor foi ativado automaticamente (BOT_OWNER_ID nao configurado).",
            ctx.guild
        ))
        return

    try:
        owner_user = await bot.fetch_user(BOT_OWNER_ID)
        embed = discord.Embed(
            title="Solicitacao de Ativacao",
            color=0x000000,
            timestamp=datetime.now(timezone.utc)
        )
        embed.add_field(name="Servidor", value=f"{ctx.guild.name}", inline=True)
        embed.add_field(name="ID", value=str(ctx.guild.id), inline=True)
        embed.add_field(name="Membros", value=str(ctx.guild.member_count), inline=True)
        embed.add_field(name="Solicitado por", value=f"{ctx.author} ({ctx.author.id})", inline=False)
        embed.set_footer(text="Chave correta informada. Aguardando aprovacao.")
        if ctx.guild.icon:
            embed.set_thumbnail(url=ctx.guild.icon.url)

        view = ApprovalView(
            guild_id=ctx.guild.id,
            guild_name=ctx.guild.name,
            requestor_name=str(ctx.author),
            requestor_id=ctx.author.id
        )
        await owner_user.send(embed=embed, view=view)

        await ctx.channel.send(embed=success_embed(
            "Solicitacao Enviada",
            "A chave foi validada!\nAguarde a aprovacao do dono do bot para comecar a usar.",
            ctx.guild
        ))
    except Exception as e:
        print(f"[ERRO] ativar: {e}", flush=True)
        await ctx.channel.send(embed=error_embed(
            "Erro ao enviar solicitacao. Contate o dono do bot diretamente.", ctx.guild))

# ─── Global Activation + Permission Check ─────────────────────────────────────

@bot.check
async def global_check(ctx):
    if not ctx.guild:
        return True
    return await check_activation(ctx)

# ─── Moderation Commands ──────────────────────────────────────────────────────

@bot.command(name="ban")
async def ban(ctx, member: discord.Member = None, *, reason="Sem motivo informado"):
    if not can_ban(ctx.guild, ctx.author):
        return await reply_and_delete(ctx, error_embed("Voce nao tem permissao para usar este comando.", ctx.guild))
    if not member:
        return await reply_and_delete(ctx, error_embed("Mencione um usuario valido.", ctx.guild))
    if has_anti_ban(ctx.guild.id, member.id):
        return await reply_and_delete(ctx, error_embed("Este membro possui Anti-Ban ativo.", ctx.guild))
    if is_protected(ctx.guild.id, member.id):
        return await reply_and_delete(ctx, error_embed("Este membro esta protegido.", ctx.guild))
    allowed, limit = check_ban_limit(ctx.guild.id, ctx.author)
    if not allowed:
        p = await get_prefix(bot, ctx.message)
        return await reply_and_delete(ctx, error_embed(
            f"Limite de banimentos atingido ({limit} bans). Use {p}resetban.", ctx.guild))
    try:
        await member.ban(reason=reason)
    except discord.Forbidden:
        return await reply_and_delete(ctx, error_embed("Sem permissao para banir este usuario.", ctx.guild))
    embed = create_embed(ctx.guild, "Membro Banido")
    embed.description = (
        f"**Usuario:** {member.mention} ({member.id})\n"
        f"**Moderador:** {ctx.author.mention} ({ctx.author.id})\n"
        f"**Motivo:** {reason}\n"
        f"**Horario:** <t:{int(datetime.now(timezone.utc).timestamp())}:F>"
    )
    await send_log(ctx.guild, "ban", embed)
    await reply_and_delete(ctx, success_embed("Banimento", f"{member} foi banido.\nMotivo: {reason}", ctx.guild))

@bot.command(name="kick")
@commands.has_permissions(kick_members=True)
async def kick(ctx, member: discord.Member = None, *, reason="Sem motivo informado"):
    if not member:
        return await reply_and_delete(ctx, error_embed("Mencione um usuario valido.", ctx.guild))
    if is_protected(ctx.guild.id, member.id):
        return await reply_and_delete(ctx, error_embed("Este membro esta protegido.", ctx.guild))
    try:
        await member.kick(reason=reason)
    except discord.Forbidden:
        return await reply_and_delete(ctx, error_embed("Sem permissao para expulsar este usuario.", ctx.guild))
    embed = create_embed(ctx.guild, "Membro Expulso")
    embed.description = (
        f"**Usuario:** {member.mention} ({member.id})\n"
        f"**Moderador:** {ctx.author.mention} ({ctx.author.id})\n"
        f"**Motivo:** {reason}\n"
        f"**Horario:** <t:{int(datetime.now(timezone.utc).timestamp())}:F>"
    )
    await send_log(ctx.guild, "kick", embed)
    await reply_and_delete(ctx, success_embed("Expulsao", f"{member} foi expulso.\nMotivo: {reason}", ctx.guild))

@bot.command(name="mute")
@commands.has_permissions(moderate_members=True)
async def mute(ctx, member: discord.Member = None, duration="10m", *, reason="Sem motivo informado"):
    if not member:
        return await reply_and_delete(ctx, error_embed("Mencione um usuario valido.", ctx.guild))
    if is_protected(ctx.guild.id, member.id):
        return await reply_and_delete(ctx, error_embed("Este membro esta protegido.", ctx.guild))
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    unit = duration[-1].lower() if duration else ""
    if unit not in units or not duration[:-1].isdigit():
        return await reply_and_delete(ctx, error_embed("Duracao invalida. Use: 10m, 1h, 1d", ctx.guild))
    seconds = int(duration[:-1]) * units[unit]
    until = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    try:
        _pending_bot_actions.add((ctx.guild.id, member.id, "mute"))
        await member.timeout(until, reason=reason)
    except discord.Forbidden:
        _pending_bot_actions.discard((ctx.guild.id, member.id, "mute"))
        return await reply_and_delete(ctx, error_embed("Sem permissao para mutar este usuario.", ctx.guild))
    embed = create_embed(ctx.guild, "Membro Mutado")
    embed.description = (
        f"**Usuario:** {member.mention} ({member.id})\n"
        f"**Moderador:** {ctx.author.mention} ({ctx.author.id})\n"
        f"**Duracao:** {duration}\n"
        f"**Motivo:** {reason}\n"
        f"**Horario:** <t:{int(datetime.now(timezone.utc).timestamp())}:F>"
    )
    await send_log(ctx.guild, "mute", embed)
    await reply_and_delete(ctx, success_embed("Mute", f"{member} foi mutado por {duration}.\nMotivo: {reason}", ctx.guild))

@bot.command(name="castigo")
@commands.has_permissions(manage_roles=True)
async def castigo(ctx, member: discord.Member = None, *, reason="Sem motivo informado"):
    if not member:
        return await reply_and_delete(ctx, error_embed("Mencione um usuario valido.", ctx.guild))
    if is_protected(ctx.guild.id, member.id):
        return await reply_and_delete(ctx, error_embed("Este membro esta protegido.", ctx.guild))
    # Busca o cargo configurado; fallback: cria/usa "castigo"
    gd = get_guild_data(ctx.guild.id)
    castigo_rid = gd.get("castigo_role_id")
    castigo_role = ctx.guild.get_role(int(castigo_rid)) if castigo_rid else None
    if not castigo_role:
        castigo_role = discord.utils.get(ctx.guild.roles, name="castigo")
    if not castigo_role:
        try:
            castigo_role = await ctx.guild.create_role(name="castigo", colour=discord.Colour(0x555555))
            for ch in ctx.guild.channels:
                if isinstance(ch, discord.TextChannel):
                    try:
                        await ch.set_permissions(castigo_role, send_messages=False, add_reactions=False)
                    except Exception:
                        pass
        except discord.Forbidden:
            return await reply_and_delete(ctx, error_embed("Sem permissao para criar cargos.", ctx.guild))
    try:
        _pending_bot_actions.add((ctx.guild.id, member.id, "castigo"))
        await member.add_roles(castigo_role, reason=reason)
    except discord.Forbidden:
        _pending_bot_actions.discard((ctx.guild.id, member.id, "castigo"))
        return await reply_and_delete(ctx, error_embed("Sem permissao para adicionar cargos.", ctx.guild))
    embed = create_embed(ctx.guild, "Membro em Castigo")
    embed.description = (
        f"**Usuario:** {member.mention} ({member.id})\n"
        f"**Moderador:** {ctx.author.mention} ({ctx.author.id})\n"
        f"**Motivo:** {reason}\n"
        f"**Horario:** <t:{int(datetime.now(timezone.utc).timestamp())}:F>"
    )
    await send_log(ctx.guild, "castigo", embed)
    await reply_and_delete(ctx, success_embed("Castigo", f"{member} foi colocado em castigo.\nMotivo: {reason}", ctx.guild))

@bot.command(name="clear")
@commands.has_permissions(manage_messages=True)
async def clear(ctx, amount: int = None):
    if not amount or amount < 1 or amount > 100:
        return await reply_and_delete(ctx, error_embed("Informe um numero entre 1 e 100.", ctx.guild))
    try:
        await ctx.message.delete()
        deleted = await ctx.channel.purge(limit=amount)
        msg = await ctx.channel.send(embed=success_embed("Limpeza", f"{len(deleted)} mensagens apagadas.", ctx.guild))
        asyncio.create_task(delete_after(msg))
    except discord.Forbidden:
        await reply_and_delete(ctx, error_embed("Sem permissao para apagar mensagens.", ctx.guild))

async def _executar_cl(message, gd):
    """Logica central do CL: apaga todas as mensagens do autor no canal."""
    author = message.author
    channel = message.channel
    deleted = 0
    try:
        await message.delete()
    except Exception:
        pass
    async for msg in channel.history(limit=None):
        if msg.author.id == author.id:
            try:
                await msg.delete()
                deleted += 1
            except (discord.NotFound, discord.Forbidden):
                pass


def _tem_permissao_cl(member, gd):
    """Retorna True se o membro tem permissao para usar cl."""
    cl_roles = gd.get("cl_roles", [])
    is_owner = member.id == member.guild.owner_id
    has_admin = member.guild_permissions.administrator
    has_cl_role = any(str(r.id) in cl_roles for r in member.roles)
    return is_owner or has_admin or has_cl_role


@bot.command(name="cl")
async def cl(ctx):
    """Apaga todas as mensagens do autor do comando no canal atual. Requer cargo CL, admin ou dono."""
    if not ctx.guild:
        return
    gd = get_guild_data(ctx.guild.id)
    p = gd.get("prefix", PREFIX)
    if not _tem_permissao_cl(ctx.author, gd):
        return await reply_and_delete(ctx, error_embed(
            f"Voce nao tem permissao para usar `{p}cl`.\n"
            f"Solicite ao dono do servidor que adicione seu cargo com `{p}clcargo add @cargo`.",
            ctx.guild
        ))
    await _executar_cl(ctx.message, gd)


@bot.command(name="clcargo")
async def clcargo(ctx, subcomando: str = None, cargo: discord.Role = None):
    """Gerencia os cargos que podem usar o comando cl."""
    if not ctx.guild:
        return
    gd = get_guild_data(ctx.guild.id)
    p = gd.get("prefix", PREFIX)

    is_owner = ctx.author.id == ctx.guild.owner_id
    has_admin = ctx.author.guild_permissions.administrator
    if not (is_owner or has_admin):
        return await reply_and_delete(ctx, error_embed(
            "Apenas o dono ou administrador do servidor pode configurar os cargos do CL.", ctx.guild))

    cl_roles = gd.setdefault("cl_roles", [])

    if subcomando is None or subcomando.lower() == "lista":
        roles_mencionados = [ctx.guild.get_role(int(r)) for r in cl_roles]
        roles_validos = [r for r in roles_mencionados if r]
        embed = create_embed(ctx.guild, "Cargos com Permissao CL")
        if roles_validos:
            embed.description = "\n".join(f"• {r.mention}" for r in roles_validos)
        else:
            embed.description = f"Nenhum cargo configurado. Apenas admins e o dono podem usar `{p}cl`."
        await reply_and_delete(ctx, embed)
        return

    sub = subcomando.lower()

    if sub == "add":
        if not cargo:
            return await reply_and_delete(ctx, error_embed(
                f"Mencione um cargo. Ex: `{p}clcargo add @Cargo`", ctx.guild))
        if str(cargo.id) not in cl_roles:
            cl_roles.append(str(cargo.id))
            update_guild_data(ctx.guild.id, gd)
        embed = create_embed(ctx.guild, "Cargo CL Adicionado")
        embed.description = f"O cargo {cargo.mention} agora pode usar `{p}cl`."
        await reply_and_delete(ctx, embed)

    elif sub in ("remove", "rem"):
        if not cargo:
            return await reply_and_delete(ctx, error_embed(
                f"Mencione um cargo. Ex: `{p}clcargo remove @Cargo`", ctx.guild))
        if str(cargo.id) in cl_roles:
            cl_roles.remove(str(cargo.id))
            update_guild_data(ctx.guild.id, gd)
        embed = create_embed(ctx.guild, "Cargo CL Removido")
        embed.description = f"O cargo {cargo.mention} nao pode mais usar `{p}cl`."
        await reply_and_delete(ctx, embed)

    else:
        await reply_and_delete(ctx, error_embed(
            f"Subcomando invalido. Use:\n"
            f"`{p}clcargo add @cargo` — permitir cargo\n"
            f"`{p}clcargo remove @cargo` — remover cargo\n"
            f"`{p}clcargo lista` — listar cargos permitidos",
            ctx.guild
        ))


@bot.command(name="clpalavra")
async def clpalavra(ctx, subcomando: str = None, *, palavra: str = None):
    """Gerencia palavras gatilho que ativam o cl como se fosse o proprio comando. Apenas admins e dono."""
    if not ctx.guild:
        return
    gd = get_guild_data(ctx.guild.id)
    p = gd.get("prefix", PREFIX)

    is_owner = ctx.author.id == ctx.guild.owner_id
    has_admin = ctx.author.guild_permissions.administrator
    if not (is_owner or has_admin):
        return await reply_and_delete(ctx, error_embed(
            "Apenas o dono ou administrador do servidor pode gerenciar gatilhos do CL.", ctx.guild))

    cl_palavras = gd.setdefault("cl_palavras", [])

    if subcomando is None or subcomando.lower() == "lista":
        embed = create_embed(ctx.guild, "Gatilhos CL Cadastrados")
        if cl_palavras:
            linhas = "\n".join(f"• `{p}{pw}`" for pw in cl_palavras)
            embed.description = linhas
            embed.set_footer(text=f"{len(cl_palavras)} gatilho(s) ativo(s) — digitar qualquer um deles executa o cl")
        else:
            embed.description = (
                f"Nenhum gatilho cadastrado.\n"
                f"Use `{p}clpalavra add <palavra>` para adicionar.\n"
                f"Ex: se adicionar `limpar`, quem tiver cargo CL pode digitar `{p}limpar` para executar o cl."
            )
        await reply_and_delete(ctx, embed)
        return

    sub = subcomando.lower()

    if sub == "add":
        if not palavra:
            return await reply_and_delete(ctx, error_embed(
                f"Informe a palavra gatilho. Ex: `{p}clpalavra add limpar`", ctx.guild))
        palavra = palavra.strip().lower()
        if palavra == "cl":
            return await reply_and_delete(ctx, error_embed(
                f"`cl` ja e o comando principal, nao precisa adicionar.", ctx.guild))
        if palavra in [pw.lower() for pw in cl_palavras]:
            return await reply_and_delete(ctx, error_embed(
                f"O gatilho `{palavra}` ja esta cadastrado.", ctx.guild))
        if len(cl_palavras) >= 30:
            return await reply_and_delete(ctx, error_embed(
                "Limite de 30 gatilhos atingido. Remova algum antes de adicionar.", ctx.guild))
        cl_palavras.append(palavra)
        update_guild_data(ctx.guild.id, gd)
        embed = create_embed(ctx.guild, "Gatilho CL Adicionado")
        embed.description = (
            f"Gatilho `{p}{palavra}` adicionado!\n"
            f"Agora quem tem cargo CL pode digitar `{p}{palavra}` para apagar todas as suas mensagens no canal."
        )
        await reply_and_delete(ctx, embed)

    elif sub in ("remove", "rem"):
        if not palavra:
            return await reply_and_delete(ctx, error_embed(
                f"Informe a palavra. Ex: `{p}clpalavra remove limpar`", ctx.guild))
        palavra = palavra.strip().lower()
        correspondente = next((pw for pw in cl_palavras if pw.lower() == palavra), None)
        if not correspondente:
            return await reply_and_delete(ctx, error_embed(
                f"O gatilho `{palavra}` nao esta cadastrado.", ctx.guild))
        cl_palavras.remove(correspondente)
        update_guild_data(ctx.guild.id, gd)
        embed = create_embed(ctx.guild, "Gatilho CL Removido")
        embed.description = f"O gatilho `{p}{palavra}` foi removido. Restam {len(cl_palavras)} gatilho(s)."
        await reply_and_delete(ctx, embed)

    elif sub == "limpar":
        cl_palavras.clear()
        update_guild_data(ctx.guild.id, gd)
        embed = create_embed(ctx.guild, "Gatilhos CL Removidos")
        embed.description = "Todos os gatilhos foram removidos. Apenas `{p}cl` continua funcionando."
        await reply_and_delete(ctx, embed)

    else:
        await reply_and_delete(ctx, error_embed(
            f"Subcomando invalido. Use:\n"
            f"`{p}clpalavra add <palavra>` — adicionar gatilho\n"
            f"`{p}clpalavra remove <palavra>` — remover gatilho\n"
            f"`{p}clpalavra limpar` — remover todos\n"
            f"`{p}clpalavra lista` — ver todos",
            ctx.guild
        ))


@bot.command(name="helpcl")
async def helpcl(ctx):
    """Mostra os gatilhos CL ativos. Apenas quem tem cargo CL pode usar."""
    if not ctx.guild:
        return
    gd = get_guild_data(ctx.guild.id)
    p = gd.get("prefix", PREFIX)

    if not _tem_permissao_cl(ctx.author, gd):
        return await reply_and_delete(ctx, error_embed(
            f"Apenas membros com cargo CL podem ver esta informacao.\n"
            f"Solicite ao dono do servidor que adicione seu cargo com `{p}clcargo add @cargo`.",
            ctx.guild
        ))

    cl_palavras = gd.get("cl_palavras", [])
    embed = create_embed(ctx.guild, "CL — Gatilhos Ativos")

    gatilhos = [f"• `{p}cl` — comando principal"] + [f"• `{p}{pw}`" for pw in cl_palavras]
    embed.description = "\n".join(gatilhos)
    embed.add_field(
        name="Como funciona",
        value="Digitar qualquer um desses comandos apaga **todas** as suas mensagens no canal.",
        inline=False
    )
    embed.set_footer(text=f"{len(cl_palavras) + 1} gatilho(s) ativo(s)")
    await reply_and_delete(ctx, embed)


@bot.command(name="lock")
@commands.has_permissions(manage_channels=True)
async def lock(ctx):
    try:
        await ctx.channel.set_permissions(ctx.guild.default_role, send_messages=False)
        await reply_and_delete(ctx, success_embed("Canal Trancado", f"{ctx.channel.name} foi trancado.", ctx.guild))
    except discord.Forbidden:
        await reply_and_delete(ctx, error_embed("Sem permissao para trancar este canal.", ctx.guild))

@bot.command(name="unlock")
@commands.has_permissions(manage_channels=True)
async def unlock(ctx):
    try:
        await ctx.channel.set_permissions(ctx.guild.default_role, send_messages=None)
        await reply_and_delete(ctx, success_embed("Canal Desbloqueado", f"{ctx.channel.name} foi desbloqueado.", ctx.guild))
    except discord.Forbidden:
        await reply_and_delete(ctx, error_embed("Sem permissao para desbloquear este canal.", ctx.guild))

@bot.command(name="nuke")
@commands.has_permissions(manage_channels=True)
async def nuke(ctx):
    try:
        ch = ctx.channel
        ch_name = ch.name
        ch_position = ch.position
        new_ch = await ch.clone(reason=f"Nuke por {ctx.author}")
        await ch.delete()
        try:
            await new_ch.edit(position=ch_position)
        except Exception:
            pass
        msg = await new_ch.send(embed=success_embed("Canal Nukado", f"Recriado por {ctx.author}.", ctx.guild))
        asyncio.create_task(delete_after(msg))
        embed = create_embed(ctx.guild, "Canal Nukado")
        embed.description = (
            f"**Canal:** {ch_name}\n"
            f"**Moderador:** {ctx.author.mention} ({ctx.author.id})\n"
            f"**Horario:** <t:{int(datetime.now(timezone.utc).timestamp())}:F>"
        )
        await send_log(ctx.guild, "nuke", embed)
    except discord.Forbidden:
        await reply_and_delete(ctx, error_embed("Sem permissao para nukar este canal.", ctx.guild))

# ─── Blacklist Commands ───────────────────────────────────────────────────────

@bot.command(name="blacklist")
@commands.has_permissions(administrator=True)
async def blacklist_add(ctx, user: discord.User = None, *, reason="Sem motivo informado"):
    if not user:
        return await reply_and_delete(ctx, error_embed("Mencione um usuario valido.", ctx.guild))
    add_blacklist(ctx.guild.id, user.id, reason, ctx.author.id, permanent=True)
    embed = create_embed(ctx.guild, "Blacklist Adicionada")
    embed.description = (
        f"**Usuario:** {user.mention} ({user.id})\n"
        f"**Moderador:** {ctx.author.mention} ({ctx.author.id})\n"
        f"**Motivo:** {reason}\n"
        f"**Tipo:** Permanente\n"
        f"**Horario:** <t:{int(datetime.now(timezone.utc).timestamp())}:F>"
    )
    await send_log(ctx.guild, "blacklist", embed)
    await reply_and_delete(ctx, success_embed("Blacklist", f"{user.mention} adicionado a blacklist permanente.", ctx.guild))

@bot.command(name="removeblacklist")
@commands.has_permissions(administrator=True)
async def blacklist_remove(ctx, user: discord.User = None):
    if not user:
        return await reply_and_delete(ctx, error_embed("Mencione um usuario valido.", ctx.guild))
    if not remove_blacklist(ctx.guild.id, user.id):
        return await reply_and_delete(ctx, error_embed("Este usuario nao esta na blacklist.", ctx.guild))
    embed = create_embed(ctx.guild, "Blacklist Removida")
    embed.description = (
        f"**Usuario:** {user.mention} ({user.id})\n"
        f"**Removido por:** {ctx.author.mention} ({ctx.author.id})\n"
        f"**Horario:** <t:{int(datetime.now(timezone.utc).timestamp())}:F>"
    )
    await send_log(ctx.guild, "blacklist", embed)
    await reply_and_delete(ctx, success_embed("Blacklist Removida", f"{user.mention} removido da blacklist.", ctx.guild))

@bot.command(name="removetempblacklist")
@commands.has_permissions(administrator=True)
async def temp_blacklist_remove(ctx, user: discord.User = None):
    if not user:
        return await reply_and_delete(ctx, error_embed("Mencione um usuario valido.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    entry = gd["blacklist"].get(str(user.id))
    if not entry or entry["permanent"]:
        return await reply_and_delete(ctx, error_embed("Este usuario nao possui blacklist temporaria.", ctx.guild))
    remove_blacklist(ctx.guild.id, user.id)
    await reply_and_delete(ctx, success_embed("Blacklist Temporaria Removida", f"{user.mention} removido.", ctx.guild))

# ─── VIP Commands ─────────────────────────────────────────────────────────────

def _has_vip_manage_perm(ctx):
    if ctx.author.id == ctx.guild.owner_id:
        return True
    gd = get_guild_data(ctx.guild.id)
    perm_role_id = gd.get("vip_perm_role_id")
    if perm_role_id and any(str(r.id) == perm_role_id for r in ctx.author.roles):
        return True
    manager_roles = gd.get("vip_manager_roles", [])
    return any(str(r.id) in manager_roles for r in ctx.author.roles)

@bot.command(name="setpermvip")
async def set_perm_vip(ctx, cargo=None):
    if ctx.author.id != ctx.guild.owner_id:
        return await reply_and_delete(ctx, error_embed("Apenas o dono do servidor pode usar este comando.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    if cargo is None:
        gd["vip_perm_role_id"] = None
        update_guild_data(ctx.guild.id, gd)
        return await reply_and_delete(ctx, success_embed("Cargo VIP Removido", "Nenhum cargo definido para gerenciar VIPs.", ctx.guild))
    role = None
    if isinstance(cargo, str):
        cargo_clean = cargo.strip("<@&>")
        if cargo_clean.isdigit():
            role = ctx.guild.get_role(int(cargo_clean))
    if role is None:
        try:
            role = await commands.RoleConverter().convert(ctx, str(cargo))
        except Exception:
            pass
    if not role:
        return await reply_and_delete(ctx, error_embed("Cargo nao encontrado. Use @ ou o ID do cargo.", ctx.guild))
    gd["vip_perm_role_id"] = str(role.id)
    update_guild_data(ctx.guild.id, gd)
    await reply_and_delete(ctx, success_embed("Cargo VIP Definido", f"O cargo {role.mention} agora pode gerenciar VIPs.", ctx.guild))

@bot.command(name="addpermcargo")
async def add_perm_cargo(ctx, cargo=None):
    if ctx.author.id != ctx.guild.owner_id:
        return await reply_and_delete(ctx, error_embed("Apenas o dono do servidor pode usar este comando.", ctx.guild))
    role = None
    if cargo:
        cargo_clean = str(cargo).strip("<@&>")
        if cargo_clean.isdigit():
            role = ctx.guild.get_role(int(cargo_clean))
    if role is None and cargo:
        try:
            role = await commands.RoleConverter().convert(ctx, str(cargo))
        except Exception:
            pass
    if not role:
        return await reply_and_delete(ctx, error_embed("Cargo nao encontrado. Use @ ou o ID do cargo.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    manager_roles = gd.setdefault("vip_manager_roles", [])
    if str(role.id) in manager_roles:
        return await reply_and_delete(ctx, error_embed("Este cargo ja tem permissao de gerenciar VIPs.", ctx.guild))
    manager_roles.append(str(role.id))
    update_guild_data(ctx.guild.id, gd)
    await reply_and_delete(ctx, success_embed("Cargo Adicionado", f"{role.mention} pode gerenciar VIPs.", ctx.guild))

@bot.command(name="removepermcargo")
async def remove_perm_cargo(ctx, cargo=None):
    if ctx.author.id != ctx.guild.owner_id:
        return await reply_and_delete(ctx, error_embed("Apenas o dono do servidor pode usar este comando.", ctx.guild))
    role = None
    if cargo:
        cargo_clean = str(cargo).strip("<@&>")
        if cargo_clean.isdigit():
            role = ctx.guild.get_role(int(cargo_clean))
    if role is None and cargo:
        try:
            role = await commands.RoleConverter().convert(ctx, str(cargo))
        except Exception:
            pass
    if not role:
        return await reply_and_delete(ctx, error_embed("Cargo nao encontrado. Use @ ou o ID do cargo.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    manager_roles = gd.get("vip_manager_roles", [])
    if str(role.id) not in manager_roles:
        return await reply_and_delete(ctx, error_embed("Este cargo nao tem permissao de gerenciar VIPs.", ctx.guild))
    manager_roles.remove(str(role.id))
    update_guild_data(ctx.guild.id, gd)
    await reply_and_delete(ctx, success_embed("Cargo Removido", f"{role.mention} nao pode mais gerenciar VIPs.", ctx.guild))

@bot.command(name="addpermvip")
async def add_perm_vip(ctx, member: discord.Member = None):
    if not _has_vip_manage_perm(ctx):
        return await reply_and_delete(ctx, error_embed("Sem permissao para adicionar VIPs.", ctx.guild))
    if not member:
        return await reply_and_delete(ctx, error_embed("Mencione um usuario valido.", ctx.guild))
    added = await add_vip(ctx.guild, member.id, ctx.author.id)
    if not added:
        return await reply_and_delete(ctx, error_embed("Este usuario ja e VIP.", ctx.guild))
    await reply_and_delete(ctx, success_embed("VIP Adicionado", f"{member.mention} agora e VIP.\nBeneficios: cargo personalizado, 5 Anti-Bans.", ctx.guild))

@bot.command(name="removepermvip")
async def remove_perm_vip(ctx, member: discord.Member = None):
    if not _has_vip_manage_perm(ctx):
        return await reply_and_delete(ctx, error_embed("Sem permissao para remover VIPs.", ctx.guild))
    if not member:
        return await reply_and_delete(ctx, error_embed("Mencione um usuario valido.", ctx.guild))
    removed = await remove_vip(ctx.guild, member.id)
    if not removed:
        return await reply_and_delete(ctx, error_embed("Este usuario nao e VIP.", ctx.guild))
    await reply_and_delete(ctx, success_embed("VIP Removido", f"{member.mention} nao e mais VIP.", ctx.guild))

@bot.command(name="vipinfo")
async def vip_info(ctx, member: discord.Member = None):
    target = member or ctx.author
    gd = get_guild_data(ctx.guild.id)
    info = gd["vip_users"].get(str(target.id))
    if not info:
        return await reply_and_delete(ctx, error_embed(f"{target} nao e VIP.", ctx.guild))
    embed = create_embed(ctx.guild, "Informacoes VIP")
    embed.add_field(name="Usuario", value=target.mention, inline=True)
    embed.add_field(name="Anti-Bans distribuidos", value=str(len(info["anti_bans"])) + "/5", inline=True)
    adder = f"<@{info['added_by']}>" if info.get("added_by") else "Desconhecido"
    embed.add_field(name="Adicionado por", value=adder, inline=True)
    embed.add_field(name="Desde", value=f"<t:{int(info['added_at'])}:R>", inline=True)
    if info.get("role_id"):
        role = ctx.guild.get_role(int(info["role_id"]))
        if role:
            embed.add_field(name="Cargo", value=role.mention, inline=True)
    await reply_and_delete(ctx, embed)

@bot.command(name="addantban")
async def add_ant_ban(ctx, member: discord.Member = None):
    if not member:
        return await reply_and_delete(ctx, error_embed("Mencione um usuario valido.", ctx.guild))
    if not is_vip(ctx.guild.id, ctx.author.id):
        return await reply_and_delete(ctx, error_embed("Apenas VIPs podem distribuir Anti-Bans.", ctx.guild))
    success = add_anti_ban(ctx.guild.id, member.id, ctx.author.id)
    if not success:
        return await reply_and_delete(ctx, error_embed("Limite de Anti-Bans atingido (maximo 5).", ctx.guild))
    await reply_and_delete(ctx, success_embed("Anti-Ban Adicionado", f"{member.mention} agora tem Anti-Ban.", ctx.guild))

@bot.command(name="removeantban")
async def remove_ant_ban(ctx, member: discord.Member = None):
    if not member:
        return await reply_and_delete(ctx, error_embed("Mencione um usuario valido.", ctx.guild))
    success = remove_anti_ban(ctx.guild.id, member.id, ctx.author.id)
    if not success:
        return await reply_and_delete(ctx, error_embed("Voce nao distribuiu Anti-Ban para este usuario.", ctx.guild))
    await reply_and_delete(ctx, success_embed("Anti-Ban Removido", f"Anti-Ban de {member.mention} removido.", ctx.guild))

@bot.command(name="addvip")
async def addvip(ctx, member: discord.Member = None):
    if not member:
        return await reply_and_delete(ctx, error_embed("Mencione um usuario valido.", ctx.guild))
    if not is_vip(ctx.guild.id, ctx.author.id):
        return await reply_and_delete(ctx, error_embed("Apenas VIPs podem usar este comando.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    vip_data = gd["vip_users"].get(str(ctx.author.id))
    if not vip_data or not vip_data.get("role_id"):
        return await reply_and_delete(ctx, error_embed("Voce nao tem um cargo VIP criado.", ctx.guild))
    role = ctx.guild.get_role(int(vip_data["role_id"]))
    if not role:
        return await reply_and_delete(ctx, error_embed("Seu cargo VIP nao foi encontrado.", ctx.guild))
    try:
        await member.add_roles(role, reason=f"VIP concedido por {ctx.author}")
    except discord.Forbidden:
        return await reply_and_delete(ctx, error_embed("Sem permissao para dar o cargo.", ctx.guild))
    await reply_and_delete(ctx, success_embed("Cargo VIP Concedido", f"{member.mention} recebeu o cargo {role.mention}.", ctx.guild))

@bot.command(name="removevip")
async def removevip(ctx, member: discord.Member = None):
    if not member:
        return await reply_and_delete(ctx, error_embed("Mencione um usuario valido.", ctx.guild))
    if not is_vip(ctx.guild.id, ctx.author.id):
        return await reply_and_delete(ctx, error_embed("Apenas VIPs podem usar este comando.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    vip_data = gd["vip_users"].get(str(ctx.author.id))
    if not vip_data or not vip_data.get("role_id"):
        return await reply_and_delete(ctx, error_embed("Voce nao tem um cargo VIP criado.", ctx.guild))
    role = ctx.guild.get_role(int(vip_data["role_id"]))
    if not role:
        return await reply_and_delete(ctx, error_embed("Seu cargo VIP nao foi encontrado.", ctx.guild))
    if role not in member.roles:
        return await reply_and_delete(ctx, error_embed(f"{member.mention} nao possui seu cargo VIP.", ctx.guild))
    try:
        await member.remove_roles(role, reason=f"VIP removido por {ctx.author}")
    except discord.Forbidden:
        return await reply_and_delete(ctx, error_embed("Sem permissao para remover o cargo.", ctx.guild))
    await reply_and_delete(ctx, success_embed("Cargo VIP Removido", f"{member.mention} perdeu o cargo {role.mention}.", ctx.guild))


async def aplicar_emoji_cargo(role: discord.Role, emoji_str: str, guild: discord.Guild):
    """
    Aplica emoji ao cargo VIP.
    Suporta emojis Unicode padrao (ex: 🎉) e emojis personalizados do servidor (ex: <:nome:123>).
    Retorna (sucesso: bool, mensagem: str)
    """
    emoji_str = emoji_str.strip()

    # Detecta emoji personalizado: <:nome:id> ou <a:nome:id> (animado)
    custom_match = re.match(r"<a?:(\w+):(\d+)>", emoji_str)
    if custom_match:
        emoji_name = custom_match.group(1)
        emoji_id = int(custom_match.group(2))

        emoji_obj = guild.get_emoji(emoji_id)
        if not emoji_obj:
            return False, (
                f"Emoji `:{emoji_name}:` nao encontrado neste servidor.\n"
                "Apenas emojis personalizados **deste servidor** podem ser usados como icone de cargo."
            )

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(str(emoji_obj.url)) as resp:
                    if resp.status != 200:
                        return False, "Nao foi possivel baixar a imagem do emoji. Tente novamente."
                    emoji_bytes = await resp.read()
            await role.edit(display_icon=emoji_bytes, reason="gerenciarvip")
            return True, f"Emoji do cargo alterado para {emoji_obj} (`:{emoji_name}:`)."
        except discord.Forbidden:
            return False, (
                "Sem permissao para definir icone no cargo.\n"
                "O servidor precisa ter **nivel 2 de boost** para usar icones personalizados em cargos."
            )
        except Exception as e:
            return False, f"Erro ao definir emoji personalizado: {e}"
    else:
        # Emoji Unicode padrao
        try:
            await role.edit(unicode_emoji=emoji_str, reason="gerenciarvip")
            return True, f"Emoji do cargo alterado para {emoji_str}."
        except discord.Forbidden:
            return False, "Sem permissao para editar o cargo."
        except Exception as e:
            return False, (
                f"Erro ao definir emoji: {e}\n"
                "Verifique se enviou um emoji valido do Discord."
            )


class GerenciarVipModal(discord.ui.Modal, title="Editar Cargo VIP"):
    def __init__(self, field: str):
        super().__init__()
        self.field = field
        if field == "nome":
            self.valor = discord.ui.TextInput(label="Novo nome do cargo", max_length=100, required=True)
        elif field == "cor":
            self.valor = discord.ui.TextInput(label="Nova cor (hex, ex: #7C3AED)", max_length=7, required=True)
        elif field == "emoji":
            self.valor = discord.ui.TextInput(
                label="Emoji do cargo",
                placeholder="Ex: 🎉  ou  <:nomeemoji:123456789>",
                max_length=100,
                required=True,
            )
        self.add_item(self.valor)

    async def on_submit(self, interaction: discord.Interaction):
        gd = get_guild_data(interaction.guild.id)
        vip_data = gd["vip_users"].get(str(interaction.user.id))
        if not vip_data or not vip_data.get("role_id"):
            return await interaction.response.send_message("Voce nao tem cargo VIP.", ephemeral=True)
        role = interaction.guild.get_role(int(vip_data["role_id"]))
        if not role:
            return await interaction.response.send_message("Cargo nao encontrado.", ephemeral=True)
        valor = self.valor.value.strip()
        try:
            if self.field == "nome":
                await role.edit(name=valor, reason="gerenciarvip")
                await interaction.response.send_message(f"Nome do cargo alterado para **{valor}**.", ephemeral=True)
            elif self.field == "cor":
                hex_val = valor.lstrip("#")
                if len(hex_val) != 6:
                    return await interaction.response.send_message("Cor invalida. Use 6 caracteres hex.", ephemeral=True)
                try:
                    color_int = int(hex_val, 16)
                except ValueError:
                    return await interaction.response.send_message("Cor invalida.", ephemeral=True)
                await role.edit(colour=discord.Colour(color_int), reason="gerenciarvip")
                await interaction.response.send_message(f"Cor do cargo alterada para `#{hex_val.upper()}`.", ephemeral=True)
            elif self.field == "emoji":
                await interaction.response.defer(ephemeral=True)
                ok, msg = await aplicar_emoji_cargo(role, valor, interaction.guild)
                await interaction.followup.send(msg, ephemeral=True)
        except discord.Forbidden:
            await interaction.response.send_message("Sem permissao para editar o cargo.", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"Erro: {e}", ephemeral=True)


class GerenciarVipView(discord.ui.View):
    def __init__(self, owner_id: int):
        super().__init__(timeout=120)
        self.owner_id = owner_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Apenas o dono do VIP pode usar este painel.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Alterar Nome", style=discord.ButtonStyle.secondary, row=0)
    async def alterar_nome(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(GerenciarVipModal("nome"))

    @discord.ui.button(label="Alterar Cor", style=discord.ButtonStyle.secondary, row=0)
    async def alterar_cor(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(GerenciarVipModal("cor"))

    @discord.ui.button(label="Alterar Emoji", style=discord.ButtonStyle.secondary, row=0)
    async def alterar_emoji(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(GerenciarVipModal("emoji"))

    @discord.ui.button(label="Ver Membros", style=discord.ButtonStyle.secondary, row=1)
    async def ver_membros(self, interaction: discord.Interaction, button: discord.ui.Button):
        gd = get_guild_data(interaction.guild.id)
        vip_data = gd["vip_users"].get(str(interaction.user.id))
        if not vip_data or not vip_data.get("role_id"):
            return await interaction.response.send_message("Voce nao tem cargo VIP.", ephemeral=True)
        role = interaction.guild.get_role(int(vip_data["role_id"]))
        if not role:
            return await interaction.response.send_message("Cargo nao encontrado.", ephemeral=True)
        membros = [m.mention for m in role.members]
        desc = "\n".join(membros) if membros else "Nenhum membro com este cargo."
        embed = create_embed(interaction.guild, f"Membros — {role.name}")
        embed.description = desc
        await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.command(name="gerenciarvip")
async def gerenciarvip(ctx):
    if not is_vip(ctx.guild.id, ctx.author.id):
        return await reply_and_delete(ctx, error_embed("Apenas VIPs podem usar este comando.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    vip_data = gd["vip_users"].get(str(ctx.author.id))
    if not vip_data or not vip_data.get("role_id"):
        return await reply_and_delete(ctx, error_embed("Voce nao tem um cargo VIP criado.", ctx.guild))
    role = ctx.guild.get_role(int(vip_data["role_id"]))
    embed = create_embed(ctx.guild, "Gerenciar Cargo VIP")
    if role:
        embed.description = (
            f"**Cargo:** {role.mention}\n"
            f"**Nome:** {role.name}\n"
            f"**Cor:** `#{role.colour.value:06X}`\n"
            f"**Membros:** {len(role.members)}\n\n"
            f"Use os botoes abaixo para editar seu cargo VIP."
        )
    else:
        embed.description = "Seu cargo VIP nao foi encontrado no servidor."
    view = GerenciarVipView(ctx.author.id)
    try:
        await ctx.message.delete()
    except Exception:
        pass
    await ctx.channel.send(embed=embed, view=view)

@bot.command(name="cargovipabaixo")
async def cargo_vip_abaixo(ctx, cargo=None):
    """
    Define o cargo de referencia que fica ACIMA de todos os cargos VIP criados pelos membros.
    Novos cargos VIP serao posicionados logo abaixo deste cargo.
    Apenas o dono do servidor pode usar.
    Sem argumento: remove a referencia.
    Uso: {prefixo}cargovipabaixo @cargo
    """
    if ctx.author.id != ctx.guild.owner_id:
        return await reply_and_delete(ctx, error_embed("Apenas o dono do servidor pode usar este comando.", ctx.guild))

    gd = get_guild_data(ctx.guild.id)

    if cargo is None:
        gd["vip_above_role_id"] = None
        update_guild_data(ctx.guild.id, gd)
        return await reply_and_delete(ctx, success_embed(
            "Referencia VIP Removida",
            "Nenhum cargo de referencia definido. Os cargos VIP serao criados na posicao padrao.",
            ctx.guild
        ))

    # Resolve o cargo por mencao ou ID
    role = None
    if isinstance(cargo, str):
        cargo_clean = cargo.strip("<@&>")
        if cargo_clean.isdigit():
            role = ctx.guild.get_role(int(cargo_clean))
    if role is None:
        try:
            role = await commands.RoleConverter().convert(ctx, str(cargo))
        except Exception:
            pass
    if not role:
        return await reply_and_delete(ctx, error_embed("Cargo nao encontrado. Use @ ou o ID do cargo.", ctx.guild))

    gd["vip_above_role_id"] = str(role.id)
    update_guild_data(ctx.guild.id, gd)
    await reply_and_delete(ctx, success_embed(
        "Cargo VIP Referencia Definido",
        f"O cargo {role.mention} agora e a referencia acima dos cargos VIP.\n"
        f"Todos os novos cargos VIP criados pelos membros serao posicionados logo **abaixo** de {role.mention}.",
        ctx.guild
    ))

@bot.command(name="helpvip")
async def helpvip(ctx):
    gd = get_guild_data(ctx.guild.id) if ctx.guild else {}
    p = gd.get("prefix", PREFIX)
    embed = create_embed(ctx.guild, "Comandos VIP")
    embed.description = (
        f"`{p}addvip @user` — dar seu cargo VIP para um membro\n"
        f"`{p}removevip @user` — remover seu cargo VIP de um membro\n"
        f"`{p}addantban @user` — distribuir Anti-Ban para um membro (max 5)\n"
        f"`{p}removeantban @user` — remover Anti-Ban de um membro\n"
        f"`{p}gerenciarvip` — painel para editar nome, cor e emoji do seu cargo\n"
        f"`{p}vipinfo [@user]` — ver informacoes do VIP\n\n"
        f"**Admin:**\n"
        f"`{p}cargovipabaixo @cargo` — define o cargo acima de todos os cargos VIP"
    )
    embed.set_footer(text="Apenas membros com status VIP podem usar estes comandos.")
    await reply_and_delete(ctx, embed)

# ─── Ban Permission Commands (Dono) ──────────────────────────────────────────

@bot.command(name="addbanrole")
async def add_ban_role(ctx, role: discord.Role = None):
    """Define um cargo com permissao para usar o comando ban. Apenas dono do servidor."""
    if not is_server_owner(ctx):
        return await reply_and_delete(ctx, error_embed("Apenas o dono do servidor pode configurar cargos de ban.", ctx.guild))
    if not role:
        return await reply_and_delete(ctx, error_embed("Mencione um cargo valido.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    ban_roles = gd.get("ban_roles", [])
    if str(role.id) in ban_roles:
        return await reply_and_delete(ctx, error_embed(f"O cargo {role.mention} ja tem permissao de ban.", ctx.guild))
    ban_roles.append(str(role.id))
    gd["ban_roles"] = ban_roles
    update_guild_data(ctx.guild.id, gd)
    await reply_and_delete(ctx, success_embed(
        "Cargo de Ban Adicionado",
        f"Membros com o cargo {role.mention} agora podem usar o comando ban.",
        ctx.guild
    ))

@bot.command(name="removebanrole")
async def remove_ban_role(ctx, role: discord.Role = None):
    """Remove a permissao de ban de um cargo. Apenas dono do servidor."""
    if not is_server_owner(ctx):
        return await reply_and_delete(ctx, error_embed("Apenas o dono do servidor pode configurar cargos de ban.", ctx.guild))
    if not role:
        return await reply_and_delete(ctx, error_embed("Mencione um cargo valido.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    ban_roles = gd.get("ban_roles", [])
    if str(role.id) not in ban_roles:
        return await reply_and_delete(ctx, error_embed(f"O cargo {role.mention} nao tem permissao de ban.", ctx.guild))
    ban_roles.remove(str(role.id))
    gd["ban_roles"] = ban_roles
    update_guild_data(ctx.guild.id, gd)
    await reply_and_delete(ctx, success_embed(
        "Cargo de Ban Removido",
        f"O cargo {role.mention} nao pode mais usar o comando ban.",
        ctx.guild
    ))

@bot.command(name="banroles")
async def ban_roles_list(ctx):
    """Lista os cargos com permissao para dar ban."""
    if not is_server_owner(ctx) and not ctx.author.guild_permissions.administrator:
        return await reply_and_delete(ctx, error_embed("Sem permissao.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    ban_roles = gd.get("ban_roles", [])
    embed = create_embed(ctx.guild, "Cargos com Permissao de Ban")
    if not ban_roles:
        embed.description = "Nenhum cargo configurado. Apenas admins com `ban_members` podem banir."
    else:
        lines = []
        for rid in ban_roles:
            role = ctx.guild.get_role(int(rid))
            lines.append(role.mention if role else f"~~Cargo removido ({rid})~~")
        embed.description = "\n".join(lines)
    await reply_and_delete(ctx, embed)

# ─── Minimum Permission Role (Dono) ──────────────────────────────────────────

@bot.command(name="setperm")
async def set_perm(ctx, role: discord.Role = None):
    """
    Define o cargo minimo para usar os comandos do bot.
    Membros com cargo abaixo desse nao podem usar o bot.
    Apenas o dono do servidor pode usar.
    Sem argumento: remove a restricao.
    """
    if not is_server_owner(ctx):
        return await reply_and_delete(ctx, error_embed("Apenas o dono do servidor pode definir o cargo minimo.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    if not role:
        gd["min_role_id"] = None
        update_guild_data(ctx.guild.id, gd)
        return await reply_and_delete(ctx, success_embed(
            "Restricao Removida",
            "Qualquer membro agora pode usar os comandos do bot.",
            ctx.guild
        ))
    gd["min_role_id"] = str(role.id)
    update_guild_data(ctx.guild.id, gd)
    await reply_and_delete(ctx, success_embed(
        "Cargo Minimo Definido",
        f"Apenas membros com {role.mention} ou superior podem usar os comandos do bot.\n"
        f"O dono do servidor sempre tem acesso total.",
        ctx.guild
    ))

# ─── Ban Limits (Dono) ────────────────────────────────────────────────────────

@bot.command(name="addpermban")
async def add_perm_ban(ctx, role: discord.Role = None, limit: int = None):
    if not is_server_owner(ctx):
        return await reply_and_delete(ctx, error_embed("Apenas o dono do servidor pode configurar limites de ban.", ctx.guild))
    if not role or not limit or limit < 1:
        return await reply_and_delete(ctx, error_embed("Uso correto: `addpermban @cargo <N>`", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    now = datetime.now(timezone.utc).timestamp()
    gd["ban_limits"][str(role.id)] = {
        "limit": limit,
        "used": 0,
        "reset_at": now + BAN_LIMIT_RESET_DAYS * 86400,
        "role_name": role.name
    }
    update_guild_data(ctx.guild.id, gd)
    await reply_and_delete(ctx, success_embed(
        "Limite de Ban Configurado",
        f"Cargo {role.mention} pode banir no maximo **{limit}x** por semana.",
        ctx.guild
    ))

@bot.command(name="editpermban")
async def edit_perm_ban(ctx, role: discord.Role = None, limit: int = None):
    if not is_server_owner(ctx):
        return await reply_and_delete(ctx, error_embed("Apenas o dono do servidor pode editar limites de ban.", ctx.guild))
    if not role or not limit or limit < 1:
        return await reply_and_delete(ctx, error_embed("Uso correto: `editpermban @cargo <N>`", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    if str(role.id) not in gd["ban_limits"]:
        return await reply_and_delete(ctx, error_embed("Este cargo nao tem limite configurado. Use addpermban.", ctx.guild))
    gd["ban_limits"][str(role.id)]["limit"] = limit
    update_guild_data(ctx.guild.id, gd)
    await reply_and_delete(ctx, success_embed("Limite Atualizado", f"Novo limite de {role.mention}: **{limit}x/semana**.", ctx.guild))

@bot.command(name="banlimits")
async def ban_limits(ctx):
    if not is_server_owner(ctx) and not ctx.author.guild_permissions.administrator:
        return await reply_and_delete(ctx, error_embed("Sem permissao.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    if not gd["ban_limits"]:
        return await reply_and_delete(ctx, error_embed("Nenhum limite de ban configurado.", ctx.guild))
    now = datetime.now(timezone.utc).timestamp()
    embed = create_embed(ctx.guild, "Painel de Limites de Banimento")
    lines = []
    for role_id, data in gd["ban_limits"].items():
        role = ctx.guild.get_role(int(role_id))
        role_name = role.name if role else data.get("role_name", f"ID:{role_id}")
        limit = data["limit"]
        used = data.get("used", 0)
        remaining = limit - used
        reset_at = data.get("reset_at", now)
        if now > reset_at:
            used = 0
            remaining = limit
        pct = int((used / limit) * 10)
        bar = "█" * pct + "░" * (10 - pct)
        lines.append(
            f"**{role_name}**\n"
            f"`{bar}` `{used}/{limit}` — **{remaining} restantes**\n"
            f"Reinicia: <t:{int(reset_at)}:R>\n"
        )
    embed.description = "\n".join(lines)
    await reply_and_delete(ctx, embed)

@bot.command(name="resetban")
async def reset_ban(ctx):
    if not is_server_owner(ctx) and not ctx.author.guild_permissions.administrator:
        return await reply_and_delete(ctx, error_embed("Sem permissao.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    now = datetime.now(timezone.utc).timestamp()
    for rid in gd["ban_limits"]:
        gd["ban_limits"][rid]["used"] = 0
        gd["ban_limits"][rid]["reset_at"] = now + BAN_LIMIT_RESET_DAYS * 86400
    update_guild_data(ctx.guild.id, gd)
    await reply_and_delete(ctx, success_embed("Limites Resetados", "Contadores resetados para todos os cargos.", ctx.guild))

# ─── Protected Commands ───────────────────────────────────────────────────────

@bot.command(name="protected")
@commands.has_permissions(administrator=True)
async def protected_cmd(ctx, member: discord.Member = None):
    if not member:
        return await reply_and_delete(ctx, error_embed("Mencione um usuario valido.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    uid = str(member.id)
    if uid in gd["protected_users"]:
        gd["protected_users"].remove(uid)
        update_guild_data(ctx.guild.id, gd)
        await reply_and_delete(ctx, success_embed("Protecao Removida", f"{member.mention} nao esta mais protegido.", ctx.guild))
    else:
        gd["protected_users"].append(uid)
        update_guild_data(ctx.guild.id, gd)
        await reply_and_delete(ctx, success_embed("Usuario Protegido", f"{member.mention} esta protegido contra ban, kick, mute e castigo.", ctx.guild))

@bot.command(name="protectedlist")
@commands.has_permissions(administrator=True)
async def protected_list(ctx):
    gd = get_guild_data(ctx.guild.id)
    protected = gd.get("protected_users", [])
    embed = create_embed(ctx.guild, "Usuarios Protegidos")
    embed.description = "\n".join(f"<@{uid}>" for uid in protected) or "Nenhum usuario protegido."
    await reply_and_delete(ctx, embed)

# ─── Anti-Raid Panel Command ─────────────────────────────────────────────────

@bot.command(name="antiraid")
async def antiraid_panel(ctx):
    if not is_server_owner(ctx):
        return await reply_and_delete(ctx, error_embed("Este painel e exclusivo para o dono do servidor.", ctx.guild))
    try:
        await ctx.message.delete()
    except Exception:
        pass
    view = AntiRaidPanelView(ctx.author.id, ctx.guild)
    await ctx.channel.send(embed=build_antiraid_embed(ctx.guild), view=view)

# ─── Create / Delete / Clear Logs (DONO) ─────────────────────────────────────

@bot.command(name="createlogs")
async def create_logs(ctx):
    if not is_server_owner(ctx):
        return await reply_and_delete(ctx, error_embed("Apenas o dono do servidor pode criar os canais de log.", ctx.guild))
    progress = await ctx.reply(embed=create_embed(ctx.guild, "Criando canais de log...", "Por favor aguarde."))
    gd = get_guild_data(ctx.guild.id)
    category = discord.utils.get(ctx.guild.categories, name="LOGS")
    if not category:
        try:
            category = await ctx.guild.create_category(
                "LOGS",
                overwrites={ctx.guild.default_role: discord.PermissionOverwrite(view_channel=False)}
            )
        except discord.Forbidden:
            await progress.delete()
            return await reply_and_delete(ctx, error_embed("Sem permissao para criar categorias.", ctx.guild))

    created, skipped = [], []
    for log_type, channel_name in LOG_CHANNELS.items():
        existing_id = gd["log_channels"].get(log_type)
        if existing_id and ctx.guild.get_channel(int(existing_id)):
            skipped.append(channel_name)
            continue
        try:
            ch = await ctx.guild.create_text_channel(
                channel_name, category=category,
                overwrites={ctx.guild.default_role: discord.PermissionOverwrite(send_messages=False, view_channel=False)}
            )
            gd["log_channels"][log_type] = str(ch.id)
            created.append(channel_name)
        except discord.Forbidden:
            skipped.append(f"{channel_name} (sem permissao)")

    update_guild_data(ctx.guild.id, gd)
    try:
        await progress.delete()
    except Exception:
        pass
    embed = create_embed(ctx.guild, "Canais de Log Criados")
    embed.description = "Configuracao concluida."
    embed.add_field(name=f"Criados ({len(created)})", value="\n".join(f"#{n}" for n in created) or "Nenhum", inline=True)
    embed.add_field(name=f"Ja existiam ({len(skipped)})", value="\n".join(f"#{n}" for n in skipped) or "Nenhum", inline=True)
    await reply_and_delete(ctx, embed)

@bot.command(name="deletelogs")
async def delete_logs(ctx):
    if not is_server_owner(ctx):
        return await reply_and_delete(ctx, error_embed("Apenas o dono do servidor pode deletar os canais de log.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    deleted_count, failed_count = 0, 0

    to_delete = set()
    for ch_id in gd["log_channels"].values():
        to_delete.add(int(ch_id))

    log_names = set(LOG_CHANNELS.values())
    for ch in ctx.guild.text_channels:
        if ch.name in log_names:
            to_delete.add(ch.id)

    for ch_id in to_delete:
        ch = ctx.guild.get_channel(ch_id)
        if ch:
            try:
                await ch.delete(reason=f"deletelogs por {ctx.author}")
                deleted_count += 1
            except discord.Forbidden:
                failed_count += 1
            except Exception:
                failed_count += 1

    gd["log_channels"] = {}
    update_guild_data(ctx.guild.id, gd)

    category = discord.utils.get(ctx.guild.categories, name="LOGS")
    if category and len(category.channels) == 0:
        try:
            await category.delete()
        except Exception:
            pass

    await reply_and_delete(ctx, success_embed(
        "Logs Deletadas",
        f"{deleted_count} canais deletados.{f' {failed_count} falharam.' if failed_count else ''}",
        ctx.guild
    ))

@bot.command(name="clearlogs")
async def clear_logs_cmd(ctx):
    if not is_server_owner(ctx):
        return await reply_and_delete(ctx, error_embed("Apenas o dono do servidor pode limpar os logs.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    cleared_count = 0
    for ch_id in gd["log_channels"].values():
        ch = ctx.guild.get_channel(int(ch_id))
        if ch:
            try:
                await ch.purge(limit=500)
                cleared_count += 1
            except Exception:
                pass
    await reply_and_delete(ctx, success_embed("Logs Limpas", f"Mensagens apagadas em {cleared_count} canais.", ctx.guild))

# ─── Config Commands (DONO) ───────────────────────────────────────────────────

@bot.command(name="setprefix")
async def set_prefix(ctx, new_prefix: str = None):
    """Apenas o dono do servidor pode alterar o prefixo."""
    if not is_server_owner(ctx):
        return await reply_and_delete(ctx, error_embed("Apenas o dono do servidor pode alterar o prefixo.", ctx.guild))
    if not new_prefix:
        return await reply_and_delete(ctx, error_embed("Informe o novo prefixo. Exemplo: `setprefix srv!`", ctx.guild))
    if len(new_prefix) > 10:
        return await reply_and_delete(ctx, error_embed("O prefixo nao pode ter mais de 10 caracteres.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    old_prefix = gd.get("prefix", PREFIX)
    gd["prefix"] = new_prefix
    update_guild_data(ctx.guild.id, gd)
    await reply_and_delete(ctx, success_embed(
        "Prefixo Atualizado",
        f"Prefixo alterado de `{old_prefix}` para `{new_prefix}`\nNovo uso: `{new_prefix}help`",
        ctx.guild
    ))

@bot.command(name="setcolor")
@commands.has_permissions(administrator=True)
async def set_color(ctx, hex_color: str = None):
    if not hex_color:
        return await reply_and_delete(ctx, error_embed("Informe uma cor hex. Exemplo: `setcolor FF0000`", ctx.guild))
    hex_color = hex_color.lstrip("#").upper()
    if len(hex_color) != 6:
        return await reply_and_delete(ctx, error_embed("Cor invalida. Use 6 digitos hex. Exemplo: `FF0000`", ctx.guild))
    try:
        int(hex_color, 16)
    except ValueError:
        return await reply_and_delete(ctx, error_embed("Cor invalida. Use apenas 0-9 e A-F.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    gd["embed_color"] = hex_color
    update_guild_data(ctx.guild.id, gd)
    embed = create_embed(ctx.guild, "Cor das Embeds Atualizada")
    embed.description = f"**Nova cor:** #{hex_color}\nAlterado por: {ctx.author.mention}"
    await reply_and_delete(ctx, embed)

@bot.command(name="resetcolor")
@commands.has_permissions(administrator=True)
async def reset_color(ctx):
    gd = get_guild_data(ctx.guild.id)
    gd["embed_color"] = None
    update_guild_data(ctx.guild.id, gd)
    await reply_and_delete(ctx, success_embed("Cor Resetada", "A cor voltou ao padrao do servidor.", ctx.guild))

# ─── General Commands ─────────────────────────────────────────────────────────

@bot.command(name="ping")
async def ping(ctx):
    latency = round(bot.latency * 1000)
    msg_latency = round((datetime.now(timezone.utc) - ctx.message.created_at).total_seconds() * 1000)
    embed = create_embed(ctx.guild, "Latencia do Bot")
    embed.add_field(name="Latencia da API", value=f"{latency}ms", inline=True)
    embed.add_field(name="Latencia da mensagem", value=f"{msg_latency}ms", inline=True)
    await reply_and_delete(ctx, embed)

@bot.command(name="reiniciar", hidden=True)
async def reiniciar(ctx):
    """Reinicia o bot (apenas dono do bot). Nao aparece no help."""
    if not is_bot_owner(ctx.author.id):
        return
    try:
        await ctx.message.delete()
    except Exception:
        pass
    try:
        await ctx.author.send(embed=success_embed("Reiniciando", "O bot sera reiniciado em instantes.", ctx.guild))
    except Exception:
        pass
    await asyncio.sleep(1)
    os._exit(0)

@bot.command(name="status")
async def status(ctx):
    members = ctx.guild.member_count
    bots = sum(1 for m in ctx.guild.members if m.bot)
    humans = members - bots
    in_voice = sum(len(ch.members) for ch in ctx.guild.voice_channels)
    gd = get_guild_data(ctx.guild.id)
    bl_count = len(gd["blacklist"])
    vip_count = len(gd["vip_users"])
    log_count = len(gd["log_channels"])
    prot_count = len(gd.get("protected_users", []))
    ar = gd.get("antiraid_settings", DEFAULT_ANTIRAID)
    modules_on = sum(1 for v in ar.values() if v)
    ban_roles_count = len(gd.get("ban_roles", []))
    min_role_id = gd.get("min_role_id")
    min_role = ctx.guild.get_role(int(min_role_id)) if min_role_id else None
    security = "Alto Risco" if bl_count > 5 else ("Moderado" if bl_count > 0 else "Seguro")
    activity = "Alta" if in_voice > 10 else ("Moderada" if in_voice > 3 else "Baixa")
    embed = create_embed(ctx.guild, f"Status — {ctx.guild.name}")
    embed.add_field(name="Membros totais", value=str(members), inline=True)
    embed.add_field(name="Usuarios", value=str(humans), inline=True)
    embed.add_field(name="Bots", value=str(bots), inline=True)
    embed.add_field(name="Em calls", value=str(in_voice), inline=True)
    embed.add_field(name="Atividade", value=activity, inline=True)
    embed.add_field(name="Seguranca", value=security, inline=True)
    embed.add_field(name="VIPs", value=str(vip_count), inline=True)
    embed.add_field(name="Blacklists", value=str(bl_count), inline=True)
    embed.add_field(name="Protegidos", value=str(prot_count), inline=True)
    embed.add_field(name="Canais de log", value=f"{log_count}/{len(LOG_CHANNELS)}", inline=True)
    embed.add_field(name="Modulos ativos", value=f"{modules_on}/{len(DEFAULT_ANTIRAID)}", inline=True)
    embed.add_field(name="Cargos de ban", value=str(ban_roles_count), inline=True)
    embed.add_field(name="Cargo minimo", value=min_role.name if min_role else "Sem restricao", inline=True)
    embed.add_field(name="Servidor ativado", value="Sim" if gd.get("activated") else "Nao", inline=True)
    embed.add_field(name="Riscos", value=f"{bl_count} na blacklist" if bl_count > 0 else "Nenhum risco", inline=False)
    await reply_and_delete(ctx, embed)

def _build_help_embed(guild, p, category: str) -> discord.Embed:
    """Retorna a embed de help para a categoria escolhida."""
    if category == "inicio":
        e = create_embed(guild, "yov! — Inicio")
        e.description = (
            "Bem-vindo ao **brocasito**, bot de moderacao e gestao do Discord.\n\n"
            "Use o menu abaixo para navegar entre as categorias de comandos.\n\n"
            f"Prefixo atual: `{p}`\n"
            f"Suporte: <https://discord.gg/RyYZAJkw6k>"
        )
        return e

    if category == "moderacao":
        e = create_embed(guild, "yov! — Moderacao")
        e.add_field(name="Punicoes", value=(
            f"`{p}ban @user [motivo]` — banir membro\n"
            f"`{p}kick @user [motivo]` — expulsar membro\n"
            f"`{p}mute @user <duracao> [motivo]` — timeout (ex: 10m, 1h)\n"
            f"`{p}unmute @user` — remover timeout\n"
            f"`{p}castigo @user [motivo]` — dar cargo de castigo\n"
            f"`{p}uncastigo @user` — remover castigo\n"
            f"`{p}mutecall @user [motivo]` — mute de voz\n"
            f"`{p}unmutecall @user` — remover mute de voz"
        ), inline=False)
        e.add_field(name="Canais", value=(
            f"`{p}clear <1-100>` — apagar mensagens\n"
            f"`{p}lock` — trancar canal\n"
            f"`{p}unlock` — destrancar canal\n"
            f"`{p}nuke` — recriar canal zerado"
        ), inline=False)
        e.add_field(name="Protecao", value=(
            f"`{p}protected @user` — ativar/desativar protecao do membro\n"
            f"`{p}protectedlist` — listar membros protegidos"
        ), inline=False)
        return e

    if category == "blacklist":
        e = create_embed(guild, "yov! — Blacklist")
        e.add_field(name="Comandos", value=(
            f"`{p}blacklist @user [motivo]` — adicionar blacklist permanente\n"
            f"`{p}removeblacklist @user` — remover da blacklist\n"
            f"`{p}removetempblacklist @user` — remover blacklist temporaria"
        ), inline=False)
        e.add_field(name="Sobre", value=(
            "Membros na blacklist perdem todos os cargos automaticamente ao entrar no servidor.\n"
            "Blacklist permanente: nao expira.\n"
            "Blacklist temporaria: configurada pelo sistema anti-ban."
        ), inline=False)
        return e

    if category == "seguranca":
        e = create_embed(guild, "yov! — Seguranca")
        e.add_field(name="Anti-Ban", value=(
            f"`{p}addantban @user` — distribuir protecao anti-ban (apenas VIPs, max 5)\n"
            f"`{p}removeantban @user` — remover protecao anti-ban\n"
            f"`{p}setantibanrole @cargo` — cargo dado automaticamente ao entrar"
        ), inline=False)
        e.add_field(name="Anti-Raid", value=(
            f"`{p}antiraid` — painel para ativar/desativar modulos de protecao\n\n"
            f"**Anti-Spam** — remove mensagens em rafada ({SPAM_LIMIT} em {SPAM_WINDOW}s)\n"
            f"**Anti-Gore** — bloqueia imagens e videos de membros inferiores\n"
            f"**Anti-Raid** — detecta acoes em massa e bane o responsavel\n"
            f"**Anti-Disconnect** — remove cargos de quem desconecta membros de call a forca"
        ), inline=False)
        return e

    if category == "vip":
        e = create_embed(guild, "yov! — VIP")
        e.add_field(name="Uso (apenas VIPs)", value=(
            f"`{p}addvip @user` — dar seu cargo VIP a um membro\n"
            f"`{p}removevip @user` — remover seu cargo VIP de um membro\n"
            f"`{p}addantban @user` — distribuir Anti-Ban para um membro (max 5)\n"
            f"`{p}removeantban @user` — remover Anti-Ban de um membro\n"
            f"`{p}gerenciarvip` — painel para editar nome, cor e emoji do seu cargo VIP\n"
            f"`{p}vipinfo [@user]` — ver informacoes do VIP\n"
            f"`{p}helpvip` — ver resumo dos comandos VIP"
        ), inline=False)
        e.add_field(name="Gestao (Dono/Gestor)", value=(
            f"`{p}addpermvip @user` — conceder status VIP a um membro\n"
            f"`{p}removepermvip @user` — remover status VIP de um membro\n"
            f"`{p}setpermvip @cargo` — definir cargo gestor de VIPs\n"
            f"`{p}addpermcargo @cargo` — adicionar cargo como gestor de VIPs\n"
            f"`{p}removepermcargo @cargo` — remover cargo gestor de VIPs\n"
            f"`{p}cargovipabaixo @cargo` — referencia de posicao para cargos VIP criados"
        ), inline=False)
        return e

    if category == "painelcargos":
        e = create_embed(guild, "yov! — Painel de Cargos")
        e.add_field(name="Uso", value=(
            f"`{p}setarcargo @membro` — abrir painel para dar/remover cargos a um membro"
        ), inline=False)
        e.add_field(name="Configuracao (Dono/Admin)", value=(
            f"`{p}addcargopanel @cargo` — adicionar cargo ao painel de setagem\n"
            f"`{p}removecargopanel @cargo` — remover cargo do painel de setagem\n"
            f"`{p}listcargopanel` — listar cargos do painel e quem pode usa-lo\n"
            f"`{p}addpermpanel @cargo` — dar permissao para usar o painel de setagem\n"
            f"`{p}removepermpanel @cargo` — remover permissao do painel de setagem"
        ), inline=False)
        return e

    if category == "permissoes":
        e = create_embed(guild, "yov! — Permissoes")
        e.add_field(name="Permissao Minima", value=(
            f"`{p}setperm @cargo` — definir cargo minimo para usar o bot\n"
            f"`{p}setperm` — remover restricao de cargo minimo"
        ), inline=False)
        e.add_field(name="Cargos de Ban", value=(
            f"`{p}addbanrole @cargo` — dar permissao de ban a um cargo\n"
            f"`{p}removebanrole @cargo` — remover permissao de ban\n"
            f"`{p}banroles` — listar cargos com permissao de ban"
        ), inline=False)
        e.add_field(name="Limite de Bans por Cargo", value=(
            f"`{p}addpermban @cargo <N>` — definir limite de bans por semana\n"
            f"`{p}editpermban @cargo <N>` — editar limite de bans de um cargo\n"
            f"`{p}banlimits` — ver painel de contadores de ban\n"
            f"`{p}resetban` — resetar todos os contadores de ban"
        ), inline=False)
        e.add_field(name="Permissao de Dono", value=(
            f"`{p}addownerperm @cargo` — conceder permissao de dono a um cargo\n"
            f"`{p}removeownerperm @cargo` — remover permissao de dono de um cargo\n"
            f"`{p}listownerperm` — listar cargos com permissao de dono"
        ), inline=False)
        return e

    if category == "cl":
        e = create_embed(guild, "yov! — CL")
        e.add_field(name="Uso", value=(
            f"`{p}cl` — apagar todas as suas mensagens no canal atual\n"
            f"`{p}helpcl` — ver todos os gatilhos CL ativos no servidor"
        ), inline=False)
        e.add_field(name="Configuracao (Admin/Dono)", value=(
            f"`{p}clcargo add @cargo` — adicionar cargo com permissao de usar cl\n"
            f"`{p}clcargo remove @cargo` — remover cargo\n"
            f"`{p}clcargo lista` — listar cargos com permissao cl\n"
            f"`{p}clpalavra add <palavra>` — adicionar palavra gatilho para cl\n"
            f"`{p}clpalavra remove <palavra>` — remover palavra gatilho\n"
            f"`{p}clpalavra limpar` — remover todos os gatilhos\n"
            f"`{p}clpalavra lista` — listar todos os gatilhos ativos"
        ), inline=False)
        return e

    if category == "tickets":
        e = create_embed(guild, "yov! — Tickets")
        e.add_field(name="Painel", value=(
            f"`{p}criarticket` — enviar o painel de abertura de tickets no canal atual"
        ), inline=False)
        e.add_field(name="Configuracao (Admin/Dono)", value=(
            f"`{p}addassumerole @cargo` — dar permissao de assumir tickets a um cargo\n"
            f"`{p}removeassumerole @cargo` — remover permissao de assumir tickets\n"
            f"`{p}assumeroles` — listar cargos com permissao de assumir tickets"
        ), inline=False)
        e.add_field(name="Sobre", value=(
            "O sistema de tickets cria canais privados automaticamente.\n"
            "Cargos de suporte definidos nas configuracoes tem acesso automatico a todos os tickets."
        ), inline=False)
        return e

    if category == "boasvindas":
        e = create_embed(guild, "yov! — Boas-vindas")
        e.add_field(name="Comandos", value=(
            f"`{p}boasvindas` — ver configuracao atual\n"
            f"`{p}boasvindas ativar` — ativar mensagem de boas-vindas\n"
            f"`{p}boasvindas desativar` — desativar mensagem\n"
            f"`{p}boasvindas canal #canal` — definir canal de boas-vindas\n"
            f"`{p}boasvindas titulo <texto>` — editar titulo da mensagem\n"
            f"`{p}boasvindas descricao <texto>` — editar descricao\n"
            f"`{p}boasvindas cor <hex>` — definir cor da embed"
        ), inline=False)
        e.add_field(name="Variaveis disponiveis", value=(
            "`{user}` — mencao do membro\n"
            "`{server}` — nome do servidor\n"
            "`{count}` — numero de membros atual"
        ), inline=False)
        return e

    if category == "sorteio":
        e = create_embed(guild, "yov! — Sorteio")
        e.add_field(name="Comandos", value=(
            f"`{p}sorteio` — criar um novo sorteio (painel interativo)\n"
            f"`{p}setsorteiorole add @cargo` — adicionar cargo que pode criar sorteios\n"
            f"`{p}setsorteiorole remove @cargo` — remover cargo\n"
            f"`{p}setsorteiorole lista` — listar cargos com permissao de sorteio"
        ), inline=False)
        e.add_field(name="Como funciona", value=(
            f"1. Use `{p}sorteio` para abrir o painel de criacao\n"
            "2. Preencha o premio, duracao e numero de ganhadores\n"
            "3. O bot cria o sorteio no canal atual\n"
            "4. Membros reagem para participar\n"
            "5. Ao encerrar, o bot sorteia e anuncia os vencedores"
        ), inline=False)
        return e

    if category == "instagram":
        e = create_embed(guild, "yov! — Instagram")
        e.add_field(name="Uso", value=(
            f"`{p}instagram [legenda]` — postar uma foto no canal de Instagram\n"
            "Anexe uma imagem ao comando para publicar."
        ), inline=False)
        e.add_field(name="Configuracao (Admin/Dono)", value=(
            f"`{p}setinstagramcanal #canal` — definir canal onde os posts serao enviados\n"
            f"`{p}setinstagramrole add @cargo` — adicionar cargo com permissao de postar\n"
            f"`{p}setinstagramrole remove @cargo` — remover cargo\n"
            f"`{p}setinstagramrole lista` — listar cargos com permissao"
        ), inline=False)
        return e

    if category == "configuracao":
        e = create_embed(guild, "yov! — Configuracao")
        e.add_field(name="Servidor", value=(
            f"`{p}setprefix <novo>` — alterar prefixo do bot\n"
            f"`{p}setcolor <hex>` — definir cor das embeds (ex: 7c3aed)\n"
            f"`{p}resetcolor` — resetar cor para o padrao\n"
            f"`{p}antiraid` — painel de modulos anti-raid\n"
            f"`{p}owner` — painel de acoes rapidas do dono"
        ), inline=False)
        e.add_field(name="Logs", value=(
            f"`{p}createlogs` — criar todos os canais de log automaticamente\n"
            f"`{p}deletelogs` — excluir todos os canais de log\n"
            f"`{p}clearlogs` — limpar mensagens de todos os canais de log"
        ), inline=False)
        e.add_field(name="Cargos do Sistema", value=(
            f"`{p}setstaffrole add @cargo` — adicionar cargo staff\n"
            f"`{p}setstaffrole remove @cargo` — remover cargo staff\n"
            f"`{p}setstaffrole lista` — listar cargos staff\n"
            f"`{p}setmuterole @cargo` — cargo usado no mute\n"
            f"`{p}setcastigorole @cargo` — cargo de castigo\n"
            f"`{p}setmutecallrole @cargo` — cargo de mute de voz\n"
            f"`{p}setantibanrole @cargo` — cargo dado ao entrar (anti-ban)"
        ), inline=False)
        e.add_field(name="Mensagem em Massa", value=(
            f"`{p}enviarmsg` — abrir painel para enviar embed por DM a todos os membros"
        ), inline=False)
        return e

    if category == "geral":
        e = create_embed(guild, "yov! — Geral")
        e.add_field(name="Comandos", value=(
            f"`{p}ping` — ver latencia do bot\n"
            f"`{p}status` — ver estatisticas do servidor\n"
            f"`{p}help` — abrir este menu de ajuda"
        ), inline=False)
        return e

    e = create_embed(guild, "yov! — Help")
    e.description = "Selecione uma categoria no menu abaixo."
    return e


class HelpSelect(discord.ui.Select):
    def __init__(self, guild, p, requester_id):
        self.guild = guild
        self.p = p
        self.requester_id = requester_id
        options = [
            discord.SelectOption(label="Inicio",           value="inicio",        description="Sobre o bot e informacoes gerais"),
            discord.SelectOption(label="Moderacao",        value="moderacao",     description="Ban, kick, mute, castigo, clear, lock, nuke..."),
            discord.SelectOption(label="Blacklist",        value="blacklist",     description="Adicionar e remover membros da blacklist"),
            discord.SelectOption(label="Seguranca",        value="seguranca",     description="Anti-ban, anti-raid, anti-spam e protecoes"),
            discord.SelectOption(label="VIP",              value="vip",           description="Sistema VIP, cargos e anti-ban"),
            discord.SelectOption(label="Painel de Cargos", value="painelcargos",  description="Painel interativo para setar cargos"),
            discord.SelectOption(label="Permissoes",       value="permissoes",    description="Cargos de ban, limites e permissao de dono"),
            discord.SelectOption(label="CL",               value="cl",            description="Limpar mensagens e gatilhos CL"),
            discord.SelectOption(label="Tickets",          value="tickets",       description="Sistema de tickets e painel de suporte"),
            discord.SelectOption(label="Boas-vindas",      value="boasvindas",    description="Mensagem automatica para novos membros"),
            discord.SelectOption(label="Sorteio",          value="sorteio",       description="Criar e gerenciar sorteios"),
            discord.SelectOption(label="Instagram",        value="instagram",     description="Postar e configurar o canal de Instagram"),
            discord.SelectOption(label="Configuracao",     value="configuracao",  description="Prefixo, cor, logs, cargos do sistema"),
            discord.SelectOption(label="Geral",            value="geral",         description="Ping, status e outros comandos gerais"),
        ]
        super().__init__(placeholder="Escolha uma categoria...", options=options, row=0)

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.requester_id:
            return await interaction.response.send_message("Apenas quem usou o help pode navegar.", ephemeral=True)
        embed = _build_help_embed(self.guild, self.p, self.values[0])
        await interaction.response.edit_message(embed=embed)


class HelpView(discord.ui.View):
    def __init__(self, guild, p, requester_id):
        super().__init__(timeout=120)
        self.msg = None
        self.add_item(HelpSelect(guild, p, requester_id))

    async def on_timeout(self):
        if self.msg:
            try:
                await self.msg.delete()
            except Exception:
                pass


@bot.command(name="help")
async def help_cmd(ctx):
    gd = get_guild_data(ctx.guild.id) if ctx.guild else {}
    p = gd.get("prefix", PREFIX)
    embed = _build_help_embed(ctx.guild, p, "inicio")
    view = HelpView(ctx.guild, p, ctx.author.id)
    try:
        await ctx.message.delete()
    except Exception:
        pass
    msg = await ctx.channel.send(embed=embed, view=view)
    view.msg = msg

# ─── Owner Panel ──────────────────────────────────────────────────────────────

class OwnerPanelView(discord.ui.View):
    def __init__(self, owner_id):
        super().__init__(timeout=60)
        self.owner_id = owner_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Apenas o dono pode usar este painel.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Fechar todos os chats", style=discord.ButtonStyle.danger)
    async def close_chats(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        for ch in interaction.guild.text_channels:
            try:
                await ch.set_permissions(interaction.guild.default_role, send_messages=False)
            except Exception:
                pass
        await interaction.followup.send(embed=create_embed(interaction.guild, "Acao Concluida", "Todos os chats foram fechados."), ephemeral=True)

    @discord.ui.button(label="Abrir todos os chats", style=discord.ButtonStyle.success)
    async def open_chats(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        for ch in interaction.guild.text_channels:
            try:
                await ch.set_permissions(interaction.guild.default_role, send_messages=None)
            except Exception:
                pass
        await interaction.followup.send(embed=create_embed(interaction.guild, "Acao Concluida", "Todos os chats foram abertos."), ephemeral=True)

    @discord.ui.button(label="Trancar todas as calls", style=discord.ButtonStyle.danger)
    async def lock_calls(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        for ch in interaction.guild.voice_channels:
            try:
                await ch.set_permissions(interaction.guild.default_role, connect=False)
            except Exception:
                pass
        await interaction.followup.send(embed=create_embed(interaction.guild, "Acao Concluida", "Todas as calls foram trancadas."), ephemeral=True)

    @discord.ui.button(label="Destrancar todas as calls", style=discord.ButtonStyle.success)
    async def unlock_calls(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        for ch in interaction.guild.voice_channels:
            try:
                await ch.set_permissions(interaction.guild.default_role, connect=None)
            except Exception:
                pass
        await interaction.followup.send(embed=create_embed(interaction.guild, "Acao Concluida", "Todas as calls foram destrancadas."), ephemeral=True)

    @discord.ui.button(label="DM de manutencao", style=discord.ButtonStyle.primary)
    async def dm_maintenance(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        sent = 0
        for member in interaction.guild.members:
            if not member.bot:
                try:
                    embed = create_embed(interaction.guild, "Aviso de Manutencao")
                    embed.description = f"O servidor **{interaction.guild.name}** esta em manutencao.\nVolte em breve."
                    await member.send(embed=embed)
                    sent += 1
                except Exception:
                    pass
        await interaction.followup.send(embed=create_embed(interaction.guild, "DM Enviado", f"Mensagem enviada para {sent} membros."), ephemeral=True)

    @discord.ui.button(label="Limpar logs", style=discord.ButtonStyle.secondary)
    async def clear_logs(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        gd = get_guild_data(interaction.guild.id)
        for ch_id in gd["log_channels"].values():
            ch = interaction.guild.get_channel(int(ch_id))
            if ch:
                try:
                    await ch.purge(limit=500)
                except Exception:
                    pass
        await interaction.followup.send(embed=create_embed(interaction.guild, "Acao Concluida", "Todos os logs foram limpos."), ephemeral=True)

@bot.command(name="owner")
async def owner_panel(ctx):
    if not is_server_owner(ctx):
        return await reply_and_delete(ctx, error_embed("Este painel e exclusivo para o dono do servidor.", ctx.guild))
    embed = create_embed(ctx.guild, "Painel Exclusivo do Dono", "Selecione uma acao abaixo.\nTodas as operacoes afetam o servidor inteiro.")
    try:
        await ctx.message.delete()
    except Exception:
        pass
    await ctx.channel.send(embed=embed, view=OwnerPanelView(ctx.author.id))

# ─── Enviar Mensagem para DMs ─────────────────────────────────────────────────

class _EnviarMsgState:
    """Guarda o estado da mensagem em construcao para cada usuario."""
    def __init__(self):
        self.descricao: str = None
        self.banner_url: str = None
        self.logo_url: str = None
        self.cor: int = 0x000000

# estado por (guild_id, user_id)
_enviarmsg_states: dict = {}


class DescricaoMsgModal(discord.ui.Modal, title="Adicionar Descricao"):
    descricao = discord.ui.TextInput(
        label="Texto da mensagem",
        style=discord.TextStyle.paragraph,
        max_length=2000,
        required=True,
        placeholder="Digite o texto que aparecera na embed..."
    )

    def __init__(self, state: _EnviarMsgState, view_msg):
        super().__init__()
        self.state = state
        self.view_msg = view_msg
        if state.descricao:
            self.descricao.default = state.descricao

    async def on_submit(self, interaction: discord.Interaction):
        self.state.descricao = self.descricao.value.strip()
        await interaction.response.send_message(
            f"Descricao definida.\n> {self.state.descricao[:100]}{'...' if len(self.state.descricao) > 100 else ''}",
            ephemeral=True
        )
        await _refresh_enviarmsg_preview(interaction, self.state, self.view_msg)


class CorHexMsgModal(discord.ui.Modal, title="Definir Cor da Embed"):
    cor = discord.ui.TextInput(
        label="Cor hex (ex: #FF0000)",
        max_length=7,
        required=True,
        placeholder="#000000"
    )

    def __init__(self, state: _EnviarMsgState, view_msg):
        super().__init__()
        self.state = state
        self.view_msg = view_msg
        self.cor.default = f"#{state.cor:06X}" if state.cor else "#000000"

    async def on_submit(self, interaction: discord.Interaction):
        cor_val = self.cor.value.strip().lstrip("#")
        if len(cor_val) != 6:
            return await interaction.response.send_message("Cor invalida. Use 6 caracteres hex, ex: `#FF0000`.", ephemeral=True)
        try:
            self.state.cor = int(cor_val, 16)
        except ValueError:
            return await interaction.response.send_message("Cor invalida.", ephemeral=True)
        await interaction.response.send_message(f"Cor definida: `#{cor_val.upper()}`.", ephemeral=True)
        await _refresh_enviarmsg_preview(interaction, self.state, self.view_msg)


class BannerMsgModal(discord.ui.Modal, title="Adicionar Banner"):
    url = discord.ui.TextInput(
        label="URL da imagem do banner",
        max_length=500,
        required=True,
        placeholder="https://..."
    )

    def __init__(self, state: _EnviarMsgState, view_msg):
        super().__init__()
        self.state = state
        self.view_msg = view_msg
        if state.banner_url:
            self.url.default = state.banner_url

    async def on_submit(self, interaction: discord.Interaction):
        self.state.banner_url = self.url.value.strip()
        await interaction.response.send_message("Banner definido.", ephemeral=True)
        await _refresh_enviarmsg_preview(interaction, self.state, self.view_msg)


class LogoMsgModal(discord.ui.Modal, title="Adicionar Logo"):
    url = discord.ui.TextInput(
        label="URL da imagem da logo",
        max_length=500,
        required=True,
        placeholder="https://..."
    )

    def __init__(self, state: _EnviarMsgState, view_msg):
        super().__init__()
        self.state = state
        self.view_msg = view_msg
        if state.logo_url:
            self.url.default = state.logo_url

    async def on_submit(self, interaction: discord.Interaction):
        self.state.logo_url = self.url.value.strip()
        await interaction.response.send_message("Logo definida.", ephemeral=True)
        await _refresh_enviarmsg_preview(interaction, self.state, self.view_msg)


async def _refresh_enviarmsg_preview(interaction: discord.Interaction, state: _EnviarMsgState, view_msg):
    """Atualiza o embed de preview do painel com o estado atual."""
    try:
        preview = _build_enviarmsg_preview(interaction.guild, state)
        await view_msg.edit(embed=preview)
    except Exception:
        pass


def _build_enviarmsg_preview(guild, state: _EnviarMsgState) -> discord.Embed:
    """Monta o embed de preview mostrando o que foi configurado ate agora."""
    embed = discord.Embed(
        title="Painel — Enviar Mensagem por DM",
        color=state.cor,
        timestamp=datetime.now(timezone.utc)
    )
    embed.add_field(
        name="Descricao",
        value=f"```{state.descricao[:200]}...```" if state.descricao and len(state.descricao) > 200
              else f"```{state.descricao}```" if state.descricao else "*(nao definida)*",
        inline=False
    )
    embed.add_field(name="Cor", value=f"`#{state.cor:06X}`" if state.cor else "*(nao definida — padrao preto)*", inline=True)
    embed.add_field(name="Banner", value=state.banner_url or "*(nao definido)*", inline=False)
    embed.add_field(name="Logo", value=state.logo_url or "*(nao definida)*", inline=False)
    embed.set_footer(text="Use os botoes abaixo para configurar a mensagem e depois clique em Enviar.")
    if state.banner_url:
        try:
            embed.set_image(url=state.banner_url)
        except Exception:
            pass
    if state.logo_url:
        try:
            embed.set_thumbnail(url=state.logo_url)
        except Exception:
            pass
    return embed


class EnviarMsgView(discord.ui.View):
    def __init__(self, owner_id: int, state: _EnviarMsgState, view_msg_ref: list):
        super().__init__(timeout=600)
        self.owner_id = owner_id
        self.state = state
        self.view_msg_ref = view_msg_ref  # lista com [Message] para poder atualizar

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("Apenas quem abriu o painel pode usar.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="➕ Adicionar Descricao", style=discord.ButtonStyle.secondary, row=0)
    async def add_descricao(self, interaction: discord.Interaction, button: discord.ui.Button):
        msg = self.view_msg_ref[0] if self.view_msg_ref else None
        await interaction.response.send_modal(DescricaoMsgModal(self.state, msg))

    @discord.ui.button(label="🖼️ Adicionar Banner", style=discord.ButtonStyle.secondary, row=0)
    async def add_banner(self, interaction: discord.Interaction, button: discord.ui.Button):
        msg = self.view_msg_ref[0] if self.view_msg_ref else None
        await interaction.response.send_modal(BannerMsgModal(self.state, msg))

    @discord.ui.button(label="🏷️ Adicionar Logo", style=discord.ButtonStyle.secondary, row=0)
    async def add_logo(self, interaction: discord.Interaction, button: discord.ui.Button):
        msg = self.view_msg_ref[0] if self.view_msg_ref else None
        await interaction.response.send_modal(LogoMsgModal(self.state, msg))

    @discord.ui.button(label="🎨 Cor Hex", style=discord.ButtonStyle.secondary, row=0)
    async def add_cor(self, interaction: discord.Interaction, button: discord.ui.Button):
        msg = self.view_msg_ref[0] if self.view_msg_ref else None
        await interaction.response.send_modal(CorHexMsgModal(self.state, msg))

    @discord.ui.button(label="📨 Enviar", style=discord.ButtonStyle.secondary, row=1)
    async def enviar(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.state.descricao:
            return await interaction.response.send_message(
                "Adicione uma descricao antes de enviar.", ephemeral=True
            )

        # Desabilita todos os botoes imediatamente
        for child in self.children:
            child.disabled = True
        sending_embed = _build_enviarmsg_preview(interaction.guild, self.state)
        sending_embed.title = "Enviando... Aguarde."
        msg = self.view_msg_ref[0] if self.view_msg_ref else None
        if msg:
            try:
                await msg.edit(embed=sending_embed, view=self)
            except Exception:
                pass
        await interaction.response.defer(ephemeral=True)

        # Monta a embed que vai para a DM de cada membro
        embed_dm = discord.Embed(
            title=f"Mensagem de {interaction.guild.name}",
            description=self.state.descricao,
            color=self.state.cor,
            timestamp=datetime.now(timezone.utc)
        )
        embed_dm.set_footer(text=f"Enviado por {interaction.user.display_name}")
        if self.state.logo_url:
            try:
                embed_dm.set_thumbnail(url=self.state.logo_url)
            except Exception:
                pass
        if self.state.banner_url:
            try:
                embed_dm.set_image(url=self.state.banner_url)
            except Exception:
                pass

        sent_members = []
        failed = 0
        for member in interaction.guild.members:
            if member.bot:
                continue
            try:
                await member.send(embed=embed_dm)
                sent_members.append(member)
                await asyncio.sleep(0.4)
            except Exception:
                failed += 1

        # Monta o embed de resultado com mencoes de quem recebeu
        mencoes = " ".join(m.mention for m in sent_members)
        # Discord limita embed description a 4096 chars
        if len(mencoes) > 3800:
            mencoes_exibido = mencoes[:3800] + f"\n*(e mais {len(mencoes[3800:].split()) } membros...)*"
        else:
            mencoes_exibido = mencoes if mencoes else "*(nenhum membro recebeu)*"

        result_embed = create_embed(
            interaction.guild,
            "Mensagem Enviada por DM",
            f"**Enviados com sucesso:** {len(sent_members)}\n"
            f"**Falharam (DM fechada):** {failed}\n\n"
            f"**Membros que receberam:**\n{mencoes_exibido}"
        )
        if msg:
            try:
                await msg.edit(embed=result_embed, view=None)
            except Exception:
                await interaction.channel.send(embed=result_embed)
        else:
            await interaction.channel.send(embed=result_embed)

        await interaction.followup.send("Mensagem enviada com sucesso!", ephemeral=True)

        # Limpa o estado
        key = (interaction.guild.id, self.owner_id)
        _enviarmsg_states.pop(key, None)

    @discord.ui.button(label="❌ Cancelar", style=discord.ButtonStyle.secondary, row=1)
    async def cancelar(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        cancel_embed = create_embed(interaction.guild, "Cancelado", "O envio de mensagens foi cancelado.")
        msg = self.view_msg_ref[0] if self.view_msg_ref else None
        if msg:
            try:
                await msg.edit(embed=cancel_embed, view=self)
            except Exception:
                pass
        await interaction.response.send_message("Cancelado.", ephemeral=True)
        key = (interaction.guild.id, self.owner_id)
        _enviarmsg_states.pop(key, None)


@bot.command(name="enviarmsg")
async def enviarmsg(ctx):
    """
    Abre o painel para montar e enviar uma embed por DM a todos os membros.
    Apenas o dono do servidor pode usar.
    """
    if not is_server_owner(ctx):
        return await reply_and_delete(ctx, error_embed("Apenas o dono do servidor pode usar este comando.", ctx.guild))

    try:
        await ctx.message.delete()
    except Exception:
        pass

    key = (ctx.guild.id, ctx.author.id)
    state = _EnviarMsgState()
    _enviarmsg_states[key] = state

    preview_embed = _build_enviarmsg_preview(ctx.guild, state)
    view_msg_ref = []
    view = EnviarMsgView(ctx.author.id, state, view_msg_ref)
    panel_msg = await ctx.channel.send(embed=preview_embed, view=view)
    view_msg_ref.append(panel_msg)

# ─── Painel de Setar Cargo ────────────────────────────────────────────────────

class CargoSelectView(discord.ui.View):
    """Painel com selects para dar/remover cargos configurados."""

    def __init__(self, target: discord.Member, panel_roles: list[discord.Role], caller: discord.Member):
        super().__init__(timeout=60)
        self.target = target
        self.caller = caller
        self.add_item(CargoSelect(target, panel_roles, caller))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.caller.id:
            await interaction.response.send_message("Apenas quem abriu o painel pode usar.", ephemeral=True)
            return False
        return True


class CargoSelect(discord.ui.Select):
    def __init__(self, target: discord.Member, panel_roles: list[discord.Role], caller: discord.Member):
        self.target = target
        self.caller = caller
        options = []
        for role in panel_roles[:25]:
            has = role in target.roles
            options.append(discord.SelectOption(
                label=role.name,
                value=str(role.id),
                description=f"{'Remover' if has else 'Dar'} este cargo",
                default=has
            ))
        super().__init__(
            placeholder="Selecione os cargos para o membro...",
            min_values=0,
            max_values=len(options),
            options=options
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild
        selected_ids = set(self.values)
        all_option_ids = {opt.value for opt in self.options}

        added = []
        removed = []

        for opt_id in all_option_ids:
            role = guild.get_role(int(opt_id))
            if not role:
                continue
            if opt_id in selected_ids:
                if role not in self.target.roles:
                    try:
                        await self.target.add_roles(role, reason=f"Painel de cargos por {interaction.user}")
                        added.append(role.name)
                    except Exception:
                        pass
            else:
                if role in self.target.roles:
                    try:
                        await self.target.remove_roles(role, reason=f"Painel de cargos por {interaction.user}")
                        removed.append(role.name)
                    except Exception:
                        pass

        linhas = []
        if added:
            linhas.append(f"**Adicionados:** {', '.join(added)}")
        if removed:
            linhas.append(f"**Removidos:** {', '.join(removed)}")
        if not linhas:
            linhas.append("Nenhuma alteracao feita.")

        result_embed = create_embed(guild, f"Cargos atualizados — {self.target.display_name}", "\n".join(linhas))
        await interaction.followup.send(embed=result_embed, ephemeral=True)


@bot.command(name="setarcargo")
async def setarcargo(ctx, membro: discord.Member = None):
    """Abre painel para setar cargos em um membro."""
    if not membro:
        return await reply_and_delete(ctx, error_embed("Mencione um membro. Ex: `yov!setarcargo @membro`", ctx.guild))

    gd = get_guild_data(ctx.guild.id)
    panel_roles_ids = gd.get("panel_roles", [])
    panel_perm_ids = gd.get("panel_perm_roles", [])

    # Verifica se quem usa tem permissao
    caller_role_ids = {str(r.id) for r in ctx.author.roles}
    has_perm = (
        ctx.guild.owner_id == ctx.author.id
        or ctx.author.guild_permissions.administrator
        or bool(caller_role_ids & set(panel_perm_ids))
    )
    if not has_perm:
        return await reply_and_delete(ctx, error_embed("Voce nao tem permissao para usar o painel de cargos.", ctx.guild))

    if not panel_roles_ids:
        return await reply_and_delete(ctx, error_embed("Nenhum cargo configurado no painel. Use `yov!addcargopanel @cargo`.", ctx.guild))

    panel_roles = [ctx.guild.get_role(int(rid)) for rid in panel_roles_ids if ctx.guild.get_role(int(rid))]
    if not panel_roles:
        return await reply_and_delete(ctx, error_embed("Os cargos configurados nao existem mais. Reconfigure com `yov!addcargopanel`.", ctx.guild))

    embed = create_embed(ctx.guild, f"Painel de Cargos — {membro.display_name}", "Selecione os cargos que deseja dar ou remover deste membro.")
    try:
        await ctx.message.delete()
    except Exception:
        pass
    await ctx.channel.send(embed=embed, view=CargoSelectView(membro, panel_roles, ctx.author))


@bot.command(name="addcargopanel")
async def addcargopanel(ctx, *cargos: discord.Role):
    """Define quais cargos aparecem no painel de setagem."""
    if not is_server_owner(ctx) and not ctx.author.guild_permissions.administrator:
        return await reply_and_delete(ctx, error_embed("Sem permissao.", ctx.guild))
    if not cargos:
        return await reply_and_delete(ctx, error_embed("Mencione ao menos um cargo.", ctx.guild))

    gd = get_guild_data(ctx.guild.id)
    panel_roles = gd.get("panel_roles", [])
    added = []
    for cargo in cargos:
        if str(cargo.id) not in panel_roles:
            panel_roles.append(str(cargo.id))
            added.append(cargo.name)
    gd["panel_roles"] = panel_roles
    update_guild_data(ctx.guild.id, gd)

    if added:
        await reply_and_delete(ctx, success_embed("Painel Atualizado", f"Cargos adicionados ao painel: {', '.join(added)}", ctx.guild))
    else:
        await reply_and_delete(ctx, error_embed("Todos os cargos ja estao no painel.", ctx.guild))


@bot.command(name="removecargopanel")
async def removecargopanel(ctx, *cargos: discord.Role):
    """Remove cargos do painel de setagem."""
    if not is_server_owner(ctx) and not ctx.author.guild_permissions.administrator:
        return await reply_and_delete(ctx, error_embed("Sem permissao.", ctx.guild))
    if not cargos:
        return await reply_and_delete(ctx, error_embed("Mencione ao menos um cargo.", ctx.guild))

    gd = get_guild_data(ctx.guild.id)
    panel_roles = gd.get("panel_roles", [])
    removed = []
    for cargo in cargos:
        if str(cargo.id) in panel_roles:
            panel_roles.remove(str(cargo.id))
            removed.append(cargo.name)
    gd["panel_roles"] = panel_roles
    update_guild_data(ctx.guild.id, gd)

    if removed:
        await reply_and_delete(ctx, success_embed("Painel Atualizado", f"Cargos removidos do painel: {', '.join(removed)}", ctx.guild))
    else:
        await reply_and_delete(ctx, error_embed("Nenhum dos cargos mencionados estava no painel.", ctx.guild))


@bot.command(name="addpermpanel")
async def addpermpanel(ctx, *cargos: discord.Role):
    """Define quais cargos podem usar o painel de setagem."""
    if not is_server_owner(ctx) and not ctx.author.guild_permissions.administrator:
        return await reply_and_delete(ctx, error_embed("Sem permissao.", ctx.guild))
    if not cargos:
        return await reply_and_delete(ctx, error_embed("Mencione ao menos um cargo.", ctx.guild))

    gd = get_guild_data(ctx.guild.id)
    panel_perm_roles = gd.get("panel_perm_roles", [])
    added = []
    for cargo in cargos:
        if str(cargo.id) not in panel_perm_roles:
            panel_perm_roles.append(str(cargo.id))
            added.append(cargo.name)
    gd["panel_perm_roles"] = panel_perm_roles
    update_guild_data(ctx.guild.id, gd)

    if added:
        await reply_and_delete(ctx, success_embed("Permissao Atualizada", f"Cargos com acesso ao painel: {', '.join(added)}", ctx.guild))
    else:
        await reply_and_delete(ctx, error_embed("Todos os cargos ja tinham permissao.", ctx.guild))


@bot.command(name="removepermpanel")
async def removepermpanel(ctx, *cargos: discord.Role):
    """Remove permissao de cargos para usar o painel."""
    if not is_server_owner(ctx) and not ctx.author.guild_permissions.administrator:
        return await reply_and_delete(ctx, error_embed("Sem permissao.", ctx.guild))
    if not cargos:
        return await reply_and_delete(ctx, error_embed("Mencione ao menos um cargo.", ctx.guild))

    gd = get_guild_data(ctx.guild.id)
    panel_perm_roles = gd.get("panel_perm_roles", [])
    removed = []
    for cargo in cargos:
        if str(cargo.id) in panel_perm_roles:
            panel_perm_roles.remove(str(cargo.id))
            removed.append(cargo.name)
    gd["panel_perm_roles"] = panel_perm_roles
    update_guild_data(ctx.guild.id, gd)

    if removed:
        await reply_and_delete(ctx, success_embed("Permissao Atualizada", f"Cargos removidos da permissao: {', '.join(removed)}", ctx.guild))
    else:
        await reply_and_delete(ctx, error_embed("Nenhum dos cargos mencionados tinha permissao.", ctx.guild))


@bot.command(name="listcargopanel")
async def listcargopanel(ctx):
    """Lista os cargos do painel e quem pode usá-los."""
    if not is_server_owner(ctx) and not ctx.author.guild_permissions.administrator:
        return await reply_and_delete(ctx, error_embed("Sem permissao.", ctx.guild))

    gd = get_guild_data(ctx.guild.id)
    panel_roles_ids = gd.get("panel_roles", [])
    panel_perm_ids = gd.get("panel_perm_roles", [])

    cargos_txt = ", ".join(
        r.mention for rid in panel_roles_ids
        if (r := ctx.guild.get_role(int(rid)))
    ) or "Nenhum configurado"
    perm_txt = ", ".join(
        r.mention for rid in panel_perm_ids
        if (r := ctx.guild.get_role(int(rid)))
    ) or "Nenhum configurado"

    embed = create_embed(ctx.guild, "Painel de Cargos — Configuracao")
    embed.add_field(name="Cargos no painel", value=cargos_txt, inline=False)
    embed.add_field(name="Cargos com permissao para setar", value=perm_txt, inline=False)
    await reply_and_delete(ctx, embed)

# ─── Permissao de Dono ────────────────────────────────────────────────────────

@bot.command(name="addownerperm")
async def add_owner_perm(ctx, *cargos: discord.Role):
    """
    Concede a cargos a mesma permissao do dono do servidor nos comandos do bot.
    Apenas o verdadeiro dono do servidor pode usar este comando.
    """
    if ctx.author.id != ctx.guild.owner_id:
        return await reply_and_delete(ctx, error_embed("Apenas o verdadeiro dono do servidor pode configurar permissoes de dono.", ctx.guild))
    if not cargos:
        return await reply_and_delete(ctx, error_embed("Mencione ao menos um cargo.", ctx.guild))

    gd = get_guild_data(ctx.guild.id)
    owner_perm_roles = gd.get("owner_perm_roles", [])
    added = []
    for cargo in cargos:
        if str(cargo.id) not in owner_perm_roles:
            owner_perm_roles.append(str(cargo.id))
            added.append(cargo.name)
    gd["owner_perm_roles"] = owner_perm_roles
    update_guild_data(ctx.guild.id, gd)

    if added:
        await reply_and_delete(ctx, success_embed(
            "Permissao de Dono Concedida",
            f"Os seguintes cargos agora podem usar os comandos de dono:\n{', '.join(added)}",
            ctx.guild
        ))
    else:
        await reply_and_delete(ctx, error_embed("Todos os cargos mencionados ja tinham permissao de dono.", ctx.guild))


@bot.command(name="removeownerperm")
async def remove_owner_perm(ctx, *cargos: discord.Role):
    """Remove a permissao de dono de cargos. Apenas o verdadeiro dono pode usar."""
    if ctx.author.id != ctx.guild.owner_id:
        return await reply_and_delete(ctx, error_embed("Apenas o verdadeiro dono do servidor pode revogar permissoes de dono.", ctx.guild))
    if not cargos:
        return await reply_and_delete(ctx, error_embed("Mencione ao menos um cargo.", ctx.guild))

    gd = get_guild_data(ctx.guild.id)
    owner_perm_roles = gd.get("owner_perm_roles", [])
    removed = []
    for cargo in cargos:
        if str(cargo.id) in owner_perm_roles:
            owner_perm_roles.remove(str(cargo.id))
            removed.append(cargo.name)
    gd["owner_perm_roles"] = owner_perm_roles
    update_guild_data(ctx.guild.id, gd)

    if removed:
        await reply_and_delete(ctx, success_embed(
            "Permissao de Dono Revogada",
            f"Os seguintes cargos nao tem mais permissao de dono:\n{', '.join(removed)}",
            ctx.guild
        ))
    else:
        await reply_and_delete(ctx, error_embed("Nenhum dos cargos mencionados tinha permissao de dono.", ctx.guild))


@bot.command(name="listownerperm")
async def list_owner_perm(ctx):
    """Lista os cargos com permissao de dono."""
    if ctx.author.id != ctx.guild.owner_id and not ctx.author.guild_permissions.administrator:
        return await reply_and_delete(ctx, error_embed("Sem permissao.", ctx.guild))

    gd = get_guild_data(ctx.guild.id)
    owner_perm_roles = gd.get("owner_perm_roles", [])

    perm_txt = ", ".join(
        r.mention for rid in owner_perm_roles
        if (r := ctx.guild.get_role(int(rid)))
    ) or "Nenhum cargo configurado"

    embed = create_embed(ctx.guild, "Permissao de Dono — Cargos Configurados")
    embed.description = perm_txt
    embed.set_footer(text="Use addownerperm @cargo para adicionar")
    await reply_and_delete(ctx, embed)

# ─── Sistema de Tickets ───────────────────────────────────────────────────────

import io as _io


def _has_assume_perm(guild, member, gd):
    """Verifica se o membro pode assumir tickets."""
    if member.id == guild.owner_id:
        return True
    if member.guild_permissions.administrator:
        return True
    assume_roles = gd.get("ticket_config", {}).get("assume_role_ids", [])
    return any(str(r.id) in assume_roles for r in member.roles)

async def _create_ticket_channel(guild, user, cfg, option_label: str = None) -> discord.TextChannel | None:
    """Cria o canal privado de ticket e retorna o objeto ou None em caso de erro."""
    gd = get_guild_data(guild.id)
    cfg["counter"] = cfg.get("counter", 0) + 1
    gd["ticket_config"] = cfg
    update_guild_data(guild.id, gd)
    ticket_num = cfg["counter"]

    category = None
    cat_id = cfg.get("category_id")
    if cat_id:
        category = guild.get_channel(int(cat_id))

    support_roles = cfg.get("support_role_ids", [])
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        user: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True),
        guild.me: discord.PermissionOverwrite(
            view_channel=True, send_messages=True, manage_channels=True, read_message_history=True
        ),
    }
    for rid in support_roles:
        role = guild.get_role(int(rid))
        if role:
            overwrites[role] = discord.PermissionOverwrite(
                view_channel=True, send_messages=True, read_message_history=True
            )

    try:
        ticket_ch = await guild.create_text_channel(
            name=f"ticket-{ticket_num:04d}",
            overwrites=overwrites,
            category=category if isinstance(category, discord.CategoryChannel) else None,
            topic=f"Ticket de {user.id} | {user}",
            reason=f"Ticket #{ticket_num:04d} aberto por {user}"
        )
    except discord.Forbidden:
        return None

    color = get_guild_color(guild)
    tipo_txt = f"\n**Tipo:** {option_label}" if option_label else ""
    custom_open_desc = cfg.get("open_description")
    if custom_open_desc:
        open_desc = custom_open_desc.replace("{user}", user.mention) + tipo_txt
    else:
        open_desc = (
            f"Ola {user.mention}, seu ticket foi aberto!{tipo_txt}\n\n"
            f"Descreva sua solicitacao com o maximo de detalhes possivel.\n"
            f"Nossa equipe respondera em breve.\n\n"
            f"Para fechar este ticket, clique em **Fechar Ticket** abaixo."
        )
    embed = discord.Embed(
        title=f"Ticket #{ticket_num:04d}",
        description=open_desc,
        color=color,
        timestamp=datetime.now(timezone.utc)
    )
    embed.set_footer(text=f"Aberto por {user} | #{ticket_num:04d}")
    if guild.icon:
        embed.set_thumbnail(url=guild.icon.url)

    close_view = TicketCloseView(user.id, guild.id, ticket_num)
    ping = user.mention + (
        " " + " ".join(f"<@&{rid}>" for rid in support_roles) if support_roles else ""
    )
    ping_msg = await ticket_ch.send(content=ping)
    asyncio.create_task(delete_after(ping_msg, 10))
    await ticket_ch.send(embed=embed, view=close_view)

    now = int(datetime.now(timezone.utc).timestamp())
    log_embed = discord.Embed(color=color, timestamp=datetime.now(timezone.utc))
    log_embed.title = "Ticket Aberto"
    log_embed.description = (
        f"**Usuario:** {user.mention} ({user.id})\n"
        f"**Canal:** {ticket_ch.mention}\n"
        f"**Numero:** #{ticket_num:04d}\n"
        + (f"**Tipo:** {option_label}\n" if option_label else "")
        + f"**Horario:** <t:{now}:F>"
    )
    await send_log(guild, "ticket", log_embed)
    return ticket_ch


def _user_has_open_ticket(guild: discord.Guild, user_id: int) -> discord.TextChannel | None:
    for ch in guild.text_channels:
        if ch.name.startswith("ticket-") and ch.topic and str(user_id) in ch.topic:
            return ch
    return None


# ─── Painel com Dropdown de Categorias ───────────────────────────────────────

class TicketCategorySelect(discord.ui.Select):
    def __init__(self, options_data: list[dict]):
        opts = []
        for item in options_data[:25]:
            emoji = item.get("emoji") or None
            label = item.get("label", "Opcao")[:100]
            try:
                opts.append(discord.SelectOption(
                    label=label,
                    value=label,
                    emoji=emoji,
                    description=item.get("description", "")[:100] or None
                ))
            except Exception:
                opts.append(discord.SelectOption(label=label, value=label))
        super().__init__(
            placeholder="Selecione uma opcao...",
            min_values=1,
            max_values=1,
            options=opts,
            custom_id="ticket_category_select"
        )

    async def callback(self, interaction: discord.Interaction):
        guild = interaction.guild
        if not guild:
            return await interaction.response.send_message("Erro.", ephemeral=True)

        existing = _user_has_open_ticket(guild, interaction.user.id)
        if existing:
            return await interaction.response.send_message(
                f"Voce ja tem um ticket aberto: {existing.mention}", ephemeral=True
            )

        await interaction.response.defer(ephemeral=True)
        gd = get_guild_data(guild.id)
        cfg = gd.get("ticket_config", {})
        option_label = self.values[0]

        ticket_ch = await _create_ticket_channel(guild, interaction.user, cfg, option_label)
        if not ticket_ch:
            return await interaction.followup.send("Sem permissao para criar canais.", ephemeral=True)
        await interaction.followup.send(f"Ticket aberto! {ticket_ch.mention}", ephemeral=True)


class TicketSelectView(discord.ui.View):
    """Painel com dropdown de categorias."""
    def __init__(self, options_data: list[dict]):
        super().__init__(timeout=None)
        self.add_item(TicketCategorySelect(options_data))


# ─── Painel com Botão simples (sem categorias) ────────────────────────────────

class TicketOpenView(discord.ui.View):
    """Painel com botão simples quando nao ha categorias configuradas."""

    def __init__(self, guild_id: int):
        super().__init__(timeout=None)
        self.guild_id = guild_id

    @discord.ui.button(label="Abrir Ticket", style=discord.ButtonStyle.secondary,
                       emoji="🎫", custom_id="ticket_open_btn")
    async def open_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        if not guild:
            return await interaction.response.send_message("Erro.", ephemeral=True)

        existing = _user_has_open_ticket(guild, interaction.user.id)
        if existing:
            return await interaction.response.send_message(
                f"Voce ja tem um ticket aberto: {existing.mention}", ephemeral=True
            )

        await interaction.response.defer(ephemeral=True)
        gd = get_guild_data(guild.id)
        cfg = gd.get("ticket_config", {})

        ticket_ch = await _create_ticket_channel(guild, interaction.user, cfg)
        if not ticket_ch:
            return await interaction.followup.send("Sem permissao para criar canais.", ephemeral=True)
        await interaction.followup.send(f"Ticket aberto! {ticket_ch.mention}", ephemeral=True)


# ─── Avaliacao por Estrelas ────────────────────────────────────────────────────

async def _do_close_ticket(guild, channel, ticket_num, opener_id, closer, rating: int | None):
    """Salva transcript, envia log e deleta o canal."""
    color = get_guild_color(guild)
    now = int(datetime.now(timezone.utc).timestamp())

    transcript_lines = [f"=== Transcript Ticket #{ticket_num:04d} ===\n"]
    try:
        async for msg in channel.history(limit=300, oldest_first=True):
            if msg.author.bot and not msg.content:
                continue
            ts = msg.created_at.strftime("%d/%m/%Y %H:%M")
            content = msg.content or ("[embed]" if msg.embeds else "")
            transcript_lines.append(f"[{ts}] {msg.author}: {content}")
    except Exception:
        pass

    stars_str = ("⭐" * rating + f" ({rating}/5)") if rating else "Sem avaliacao"

    transcript_text = "\n".join(transcript_lines) + f"\n\n[Avaliacao: {stars_str}]"
    transcript_file = discord.File(
        fp=_io.BytesIO(transcript_text.encode("utf-8")),
        filename=f"ticket-{ticket_num:04d}.txt"
    )

    log_embed = discord.Embed(
        title="Ticket Fechado",
        color=color,
        timestamp=datetime.now(timezone.utc)
    )
    log_embed.description = (
        f"**Canal:** {channel.name}\n"
        f"**Numero:** #{ticket_num:04d}\n"
        f"**Aberto por:** <@{opener_id}>\n"
        f"**Fechado por:** {closer.mention} ({closer.id})\n"
        f"**Avaliacao:** {stars_str}\n"
        f"**Horario:** <t:{now}:F>"
    )

    gd_info = get_guild_data(guild.id)
    ch_id = gd_info["log_channels"].get("ticket")
    log_ch = guild.get_channel(int(ch_id)) if ch_id else None
    if not log_ch:
        log_ch = discord.utils.get(guild.text_channels, name="log-tickets")
    if log_ch:
        try:
            await log_ch.send(embed=log_embed, file=transcript_file)
        except Exception:
            try:
                await log_ch.send(embed=log_embed)
            except Exception:
                pass

    closing_embed = discord.Embed(
        title="Encerrando ticket...",
        description="Este canal sera deletado em **5 segundos**.",
        color=color
    )
    try:
        await channel.send(embed=closing_embed)
    except Exception:
        pass

    await asyncio.sleep(5)
    try:
        await channel.delete(reason=f"Ticket #{ticket_num:04d} fechado por {closer}")
    except Exception:
        pass


class TicketRatingView(discord.ui.View):
    """Pergunta a avaliacao antes de fechar. Visivel apenas para quem abriu."""

    def __init__(self, opener_id: int, guild_id: int, ticket_num: int):
        super().__init__(timeout=60)
        self.opener_id = opener_id
        self.guild_id = guild_id
        self.ticket_num = ticket_num
        self._rated = False

        for star in range(1, 6):
            btn = discord.ui.Button(
                label="⭐" * star,
                style=discord.ButtonStyle.secondary,
                custom_id=f"rating_{star}_{ticket_num}",
                row=0
            )
            btn.callback = self._make_rating_cb(star)
            self.add_item(btn)

        skip_btn = discord.ui.Button(
            label="Pular avaliacao",
            style=discord.ButtonStyle.secondary,
            custom_id=f"rating_skip_{ticket_num}",
            row=1
        )
        skip_btn.callback = self._make_rating_cb(None)
        self.add_item(skip_btn)

    def _make_rating_cb(self, stars: int | None):
        async def _cb(interaction: discord.Interaction):
            if interaction.user.id != self.opener_id:
                return await interaction.response.send_message(
                    "Apenas quem abriu o ticket pode avaliar.", ephemeral=True
                )
            if self._rated:
                return await interaction.response.send_message("Ja avaliado.", ephemeral=True)
            self._rated = True

            for child in self.children:
                child.disabled = True
            result_label = ("⭐" * stars + f"  {stars}/5") if stars else "Sem avaliacao"
            await interaction.response.edit_message(
                content=f"Obrigado pela avaliacao: **{result_label}**\nFechando em 5s...",
                view=self
            )

            await _do_close_ticket(
                interaction.guild,
                interaction.channel,
                self.ticket_num,
                self.opener_id,
                interaction.user,
                stars
            )
        return _cb

    async def on_timeout(self):
        if not self._rated:
            try:
                guild = bot.get_guild(self.guild_id)
                if guild:
                    for ch in guild.text_channels:
                        if (ch.name == f"ticket-{self.ticket_num:04d}"
                                and ch.topic and str(self.opener_id) in ch.topic):
                            closer = guild.get_member(self.opener_id) or guild.me
                            await _do_close_ticket(guild, ch, self.ticket_num, self.opener_id, closer, None)
                            break
            except Exception:
                pass


class TicketCloseView(discord.ui.View):
    """Botoes de gerenciamento dentro do canal de ticket."""

    def __init__(self, opener_id: int, guild_id: int, ticket_num: int):
        super().__init__(timeout=None)
        self.opener_id = opener_id
        self.guild_id = guild_id
        self.ticket_num = ticket_num
        self._assumed_by: int | None = None

    async def _can_close(self, interaction: discord.Interaction) -> bool:
        guild = interaction.guild
        if not guild:
            return False
        gd = get_guild_data(guild.id)
        cfg = gd.get("ticket_config", {})
        support_roles = cfg.get("support_role_ids", [])
        member = interaction.user
        if member.id == self.opener_id:
            return True
        if member.guild_permissions.administrator or member.id == guild.owner_id:
            return True
        if any(str(r.id) in support_roles for r in member.roles):
            return True
        return False

    @discord.ui.button(label="Fechar Ticket", style=discord.ButtonStyle.secondary,
                       custom_id="ticket_close_btn", row=0)
    async def close_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._can_close(interaction):
            return await interaction.response.send_message(
                "Apenas quem abriu o ticket ou a equipe de suporte pode fechar.", ephemeral=True
            )

        guild = interaction.guild
        color = get_guild_color(guild)

        button.disabled = True
        await interaction.response.edit_message(view=self)

        rating_embed = discord.Embed(
            title="Como foi o atendimento?",
            description=(
                f"{interaction.user.mention}, avalie o atendimento deste ticket.\n"
                f"Clique em uma das estrelas abaixo.\n\n"
                f"A avaliacao expira em **60 segundos** — se nao avaliar, o ticket sera fechado sem nota."
            ),
            color=color,
            timestamp=datetime.now(timezone.utc)
        )

        rating_view = TicketRatingView(self.opener_id, guild.id, self.ticket_num)

        is_opener = interaction.user.id == self.opener_id
        if is_opener:
            await interaction.channel.send(
                content=f"<@{self.opener_id}>",
                embed=rating_embed,
                view=rating_view
            )
        else:
            member = guild.get_member(self.opener_id)
            if member:
                await interaction.channel.send(
                    content=f"<@{self.opener_id}>",
                    embed=rating_embed,
                    view=rating_view
                )
            else:
                await _do_close_ticket(
                    guild, interaction.channel, self.ticket_num,
                    self.opener_id, interaction.user, None
                )

    @discord.ui.button(label="Assumir Ticket", style=discord.ButtonStyle.secondary,
                       custom_id="ticket_assume_btn", row=1)
    async def assume_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        if not guild:
            return await interaction.response.send_message("Erro.", ephemeral=True)
        gd = get_guild_data(guild.id)
        member = interaction.user
        if not _has_assume_perm(guild, member, gd):
            return await interaction.response.send_message(
                "Voce nao tem permissao para assumir tickets.", ephemeral=True
            )
        if self._assumed_by:
            return await interaction.response.send_message(
                f"Este ticket ja foi assumido por <@{self._assumed_by}>.", ephemeral=True
            )
        self._assumed_by = member.id
        button.disabled = True
        button.label = f"Assumido por {member.display_name}"
        await interaction.response.edit_message(view=self)

        color = get_guild_color(guild)
        embed = discord.Embed(
            title="Ticket Assumido",
            description=(
                f"{member.mention} assumiu este ticket.\n"
                f"Aberto por <@{self.opener_id}>."
            ),
            color=color,
            timestamp=datetime.now(timezone.utc)
        )
        assume_notif = await interaction.channel.send(
            content=f"{member.mention} <@{self.opener_id}>",
            embed=embed
        )
        asyncio.create_task(delete_after(assume_notif, 10))

    @discord.ui.button(label="Painel", style=discord.ButtonStyle.secondary,
                       custom_id="ticket_panel_btn", row=1)
    async def open_panel(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        if not guild:
            return await interaction.response.send_message("Erro.", ephemeral=True)
        gd = get_guild_data(guild.id)
        member = interaction.user
        cfg = gd.get("ticket_config", {})
        support_roles = cfg.get("support_role_ids", [])
        assume_roles = cfg.get("assume_role_ids", [])
        has_perm = (
            member.id == guild.owner_id
            or member.guild_permissions.administrator
            or any(str(r.id) in support_roles for r in member.roles)
            or any(str(r.id) in assume_roles for r in member.roles)
        )
        if not has_perm:
            return await interaction.response.send_message(
                "Voce nao tem permissao para usar o painel do ticket.", ephemeral=True
            )
        panel_view = TicketPanelView(
            channel=interaction.channel,
            opener_id=self.opener_id,
            guild_id=guild.id,
            ticket_num=self.ticket_num,
            requester_id=member.id
        )
        await interaction.response.send_message(
            "Selecione uma acao:",
            view=panel_view,
            ephemeral=True
        )


# ─── Painel de Gerenciamento do Ticket ────────────────────────────────────────

class _AddMemberSelect(discord.ui.UserSelect):
    """Selecao de usuario para adicionar ao ticket."""

    def __init__(self, channel: discord.TextChannel, requester_id: int):
        super().__init__(
            placeholder="Selecione o membro para adicionar...",
            min_values=1,
            max_values=1,
        )
        self.ticket_channel = channel
        self.requester_id = requester_id

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.requester_id:
            return await interaction.response.send_message("Sem permissao.", ephemeral=True)
        user = self.values[0]
        try:
            await self.ticket_channel.set_permissions(
                user,
                view_channel=True,
                send_messages=True,
                read_message_history=True
            )
        except discord.Forbidden:
            return await interaction.response.edit_message(
                content="Sem permissao para alterar o canal.", view=None
            )
        color = get_guild_color(interaction.guild)
        notif = discord.Embed(
            description=f"{interaction.user.mention} adicionou {user.mention} ao ticket.",
            color=color,
            timestamp=datetime.now(timezone.utc)
        )
        notif_msg = await self.ticket_channel.send(embed=notif)
        asyncio.create_task(delete_after(notif_msg, 10))
        await interaction.response.edit_message(
            content=f"{user.mention} foi adicionado ao ticket.", view=None
        )


class _AddMemberView(discord.ui.View):
    def __init__(self, channel: discord.TextChannel, requester_id: int):
        super().__init__(timeout=60)
        self.add_item(_AddMemberSelect(channel, requester_id))


class _RemoveMemberSelect(discord.ui.UserSelect):
    """Selecao de usuario para remover do ticket."""

    def __init__(self, channel: discord.TextChannel, opener_id: int, requester_id: int):
        super().__init__(
            placeholder="Selecione o membro para remover...",
            min_values=1,
            max_values=1,
        )
        self.ticket_channel = channel
        self.opener_id = opener_id
        self.requester_id = requester_id

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.requester_id:
            return await interaction.response.send_message("Sem permissao.", ephemeral=True)
        user = self.values[0]
        if user.id == self.opener_id:
            return await interaction.response.edit_message(
                content="Nao e possivel remover quem abriu o ticket.", view=None
            )
        if user.id == interaction.guild.me.id:
            return await interaction.response.edit_message(
                content="Nao e possivel remover o bot do ticket.", view=None
            )
        try:
            await self.ticket_channel.set_permissions(
                user,
                view_channel=False,
                send_messages=False,
                read_message_history=False
            )
        except discord.Forbidden:
            return await interaction.response.edit_message(
                content="Sem permissao para alterar o canal.", view=None
            )
        color = get_guild_color(interaction.guild)
        notif = discord.Embed(
            description=f"{interaction.user.mention} removeu {user.mention} do ticket.",
            color=color,
            timestamp=datetime.now(timezone.utc)
        )
        notif_msg = await self.ticket_channel.send(embed=notif)
        asyncio.create_task(delete_after(notif_msg, 10))
        await interaction.response.edit_message(
            content=f"{user.mention} foi removido do ticket.", view=None
        )


class _RemoveMemberView(discord.ui.View):
    def __init__(self, channel: discord.TextChannel, opener_id: int, requester_id: int):
        super().__init__(timeout=60)
        self.add_item(_RemoveMemberSelect(channel, opener_id, requester_id))


class _TransferSelect(discord.ui.UserSelect):
    """Selecao de usuario para transferir o ticket."""

    def __init__(self, channel: discord.TextChannel, opener_id: int, requester_id: int):
        super().__init__(
            placeholder="Selecione o novo responsavel...",
            min_values=1,
            max_values=1,
        )
        self.ticket_channel = channel
        self.opener_id = opener_id
        self.requester_id = requester_id

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.requester_id:
            return await interaction.response.send_message("Sem permissao.", ephemeral=True)
        new_owner = self.values[0]
        if new_owner.bot:
            return await interaction.response.edit_message(
                content="Nao e possivel transferir o ticket para um bot.", view=None
            )
        guild = interaction.guild
        try:
            await self.ticket_channel.edit(
                topic=f"Ticket de {new_owner.id} | {new_owner}"
            )
            await self.ticket_channel.set_permissions(
                new_owner,
                view_channel=True,
                send_messages=True,
                read_message_history=True
            )
        except discord.Forbidden:
            return await interaction.response.edit_message(
                content="Sem permissao para alterar o canal.", view=None
            )
        color = get_guild_color(guild)
        notif = discord.Embed(
            title="Ticket Transferido",
            description=(
                f"O ticket foi transferido para {new_owner.mention}.\n"
                f"Responsavel anterior: <@{self.opener_id}>."
            ),
            color=color,
            timestamp=datetime.now(timezone.utc)
        )
        await self.ticket_channel.send(
            content=f"{new_owner.mention} <@{self.opener_id}>",
            embed=notif
        )
        await interaction.response.edit_message(
            content=f"Ticket transferido para {new_owner.mention}.", view=None
        )


class _TransferView(discord.ui.View):
    def __init__(self, channel: discord.TextChannel, opener_id: int, requester_id: int):
        super().__init__(timeout=60)
        self.add_item(_TransferSelect(channel, opener_id, requester_id))


class TicketPanelView(discord.ui.View):
    """Painel de gerenciamento interno do ticket."""

    def __init__(self, channel: discord.TextChannel, opener_id: int,
                 guild_id: int, ticket_num: int, requester_id: int):
        super().__init__(timeout=60)
        self.ticket_channel = channel
        self.opener_id = opener_id
        self.guild_id = guild_id
        self.ticket_num = ticket_num
        self.requester_id = requester_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message(
                "Apenas quem abriu este painel pode usa-lo.", ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Adicionar Membro", style=discord.ButtonStyle.secondary, row=0)
    async def add_member(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = _AddMemberView(self.ticket_channel, self.requester_id)
        await interaction.response.send_message(
            "Selecione o membro a adicionar:", view=view, ephemeral=True
        )

    @discord.ui.button(label="Remover Pessoa", style=discord.ButtonStyle.secondary, row=0)
    async def remove_member(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = _RemoveMemberView(self.ticket_channel, self.opener_id, self.requester_id)
        await interaction.response.send_message(
            "Selecione o membro a remover:", view=view, ephemeral=True
        )

    @discord.ui.button(label="Passar Ticket", style=discord.ButtonStyle.secondary, row=0)
    async def transfer_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = _TransferView(self.ticket_channel, self.opener_id, self.requester_id)
        await interaction.response.send_message(
            "Selecione o novo responsavel:", view=view, ephemeral=True
        )


@bot.group(name="setticket", invoke_without_command=True)
async def setticket(ctx):
    """Configura o sistema de tickets."""
    if not ctx.guild:
        return
    if not is_server_owner(ctx) and not ctx.author.guild_permissions.administrator:
        return await reply_and_delete(ctx, error_embed("Apenas o dono do servidor ou admins podem configurar tickets.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    cfg = gd.get("ticket_config", {})
    p = gd.get("prefix", PREFIX)

    cat = ctx.guild.get_channel(int(cfg["category_id"])) if cfg.get("category_id") else None
    support_roles_txt = ", ".join(
        f"<@&{rid}>" for rid in cfg.get("support_role_ids", [])
        if ctx.guild.get_role(int(rid))
    ) or "Nenhum"

    options = cfg.get("options", [])
    opts_txt = "\n".join(
        f"`{i+1}.` {o.get('emoji','') } {o.get('label','')}" for i, o in enumerate(options)
    ) or "Nenhuma (painel exibe botao simples)"

    embed = create_embed(ctx.guild, "Configuracao do Sistema de Tickets")
    embed.description = (
        f"**Titulo:** {cfg.get('title', '—')}\n"
        f"**Descricao:** {cfg.get('description', '—')[:80]}...\n"
        f"**Categoria:** {cat.name if cat else 'Raiz do servidor'}\n"
        f"**Cargos de suporte:** {support_roles_txt}\n"
        f"**Tickets abertos:** {cfg.get('counter', 0)}\n\n"
        f"**Opcoes do dropdown:**\n{opts_txt}\n\n"
        f"**Comandos:**\n"
        f"`{p}setticket titulo <texto>`\n"
        f"`{p}setticket descricao <texto>`\n"
        f"`{p}setticket categoria #categoria`\n"
        f"`{p}setticket suporte @cargo`\n"
        f"`{p}setticket opcao add <emoji> <nome>` — ex: `{p}setticket opcao add 💀 duvidas`\n"
        f"`{p}setticket opcao remove <numero>` — ex: `{p}setticket opcao remove 1`\n"
        f"`{p}setticket opcao limpar` — remove todas as opcoes\n"
        f"`{p}criarticket` — envia o painel neste canal"
    )
    await reply_and_delete(ctx, embed)


@setticket.command(name="titulo")
async def setticket_titulo(ctx, *, titulo: str = None):
    if not is_server_owner(ctx) and not ctx.author.guild_permissions.administrator:
        return await reply_and_delete(ctx, error_embed("Sem permissao.", ctx.guild))
    if not titulo:
        return await reply_and_delete(ctx, error_embed("Informe o titulo.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    gd["ticket_config"]["title"] = titulo[:256]
    update_guild_data(ctx.guild.id, gd)
    await reply_and_delete(ctx, success_embed("Titulo Atualizado", f"Novo titulo: **{titulo[:256]}**", ctx.guild))


@setticket.command(name="descricao")
async def setticket_descricao(ctx, *, descricao: str = None):
    if not is_server_owner(ctx) and not ctx.author.guild_permissions.administrator:
        return await reply_and_delete(ctx, error_embed("Sem permissao.", ctx.guild))
    if not descricao:
        return await reply_and_delete(ctx, error_embed("Informe a descricao.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    gd["ticket_config"]["description"] = descricao[:2048]
    update_guild_data(ctx.guild.id, gd)
    await reply_and_delete(ctx, success_embed("Descricao Atualizada", "Descricao do painel atualizada.", ctx.guild))


@setticket.command(name="categoria")
async def setticket_categoria(ctx, categoria: discord.CategoryChannel = None):
    if not is_server_owner(ctx) and not ctx.author.guild_permissions.administrator:
        return await reply_and_delete(ctx, error_embed("Sem permissao.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    if not categoria:
        gd["ticket_config"]["category_id"] = None
        update_guild_data(ctx.guild.id, gd)
        return await reply_and_delete(ctx, success_embed("Categoria Removida", "Tickets criados na raiz.", ctx.guild))
    gd["ticket_config"]["category_id"] = str(categoria.id)
    update_guild_data(ctx.guild.id, gd)
    await reply_and_delete(ctx, success_embed("Categoria Definida", f"Tickets em: **{categoria.name}**", ctx.guild))


@setticket.command(name="abertura")
async def setticket_abertura(ctx, *, texto: str = None):
    """Define a descricao da embed enviada dentro do ticket ao ser aberto. Use {user} para mencionar quem abriu."""
    if not is_server_owner(ctx) and not ctx.author.guild_permissions.administrator:
        return await reply_and_delete(ctx, error_embed("Sem permissao.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    p = gd.get("prefix", PREFIX)
    if not texto:
        atual = gd["ticket_config"].get("open_description")
        gd["ticket_config"]["open_description"] = None
        update_guild_data(ctx.guild.id, gd)
        return await reply_and_delete(ctx, success_embed(
            "Descricao de Abertura Resetada",
            "A descricao voltou ao texto padrao.",
            ctx.guild
        ))
    if len(texto) > 2048:
        return await reply_and_delete(ctx, error_embed("Texto muito longo (maximo 2048 caracteres).", ctx.guild))
    gd["ticket_config"]["open_description"] = texto
    update_guild_data(ctx.guild.id, gd)
    preview = texto.replace("{user}", ctx.author.mention)
    embed = success_embed(
        "Descricao de Abertura Atualizada",
        f"**Previa:**\n{preview[:500]}{'...' if len(preview) > 500 else ''}\n\n"
        f"Use `{{user}}` para mencionar quem abriu o ticket.\n"
        f"Para resetar: `{p}setticket abertura` (sem texto).",
        ctx.guild
    )
    await reply_and_delete(ctx, embed)


@setticket.command(name="suporte")
async def setticket_suporte(ctx, cargo: discord.Role = None):
    if not is_server_owner(ctx) and not ctx.author.guild_permissions.administrator:
        return await reply_and_delete(ctx, error_embed("Sem permissao.", ctx.guild))
    if not cargo:
        return await reply_and_delete(ctx, error_embed("Mencione um cargo.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    support_roles = gd["ticket_config"].get("support_role_ids", [])
    if str(cargo.id) in support_roles:
        support_roles.remove(str(cargo.id))
        msg = f"{cargo.mention} removido dos cargos de suporte."
    else:
        support_roles.append(str(cargo.id))
        msg = f"{cargo.mention} adicionado como cargo de suporte."
    gd["ticket_config"]["support_role_ids"] = support_roles
    update_guild_data(ctx.guild.id, gd)
    await reply_and_delete(ctx, success_embed("Suporte Atualizado", msg, ctx.guild))


@setticket.group(name="opcao", invoke_without_command=True)
async def setticket_opcao(ctx):
    """Gerencia as opcoes do dropdown de tickets."""
    if not is_server_owner(ctx) and not ctx.author.guild_permissions.administrator:
        return await reply_and_delete(ctx, error_embed("Sem permissao.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    cfg = gd.get("ticket_config", {})
    options = cfg.get("options", [])
    p = gd.get("prefix", PREFIX)
    if not options:
        return await reply_and_delete(ctx, create_embed(ctx.guild, "Opcoes do Dropdown",
            f"Nenhuma opcao configurada.\nUse `{p}setticket opcao add <emoji> <nome>` para adicionar."))
    lines = [f"`{i+1}.` {o.get('emoji','')} **{o.get('label','')}**" for i, o in enumerate(options)]
    embed = create_embed(ctx.guild, "Opcoes do Dropdown de Tickets", "\n".join(lines))
    await reply_and_delete(ctx, embed)


@setticket_opcao.command(name="add")
async def setticket_opcao_add(ctx, emoji: str = None, *, nome: str = None):
    """Adiciona uma opcao ao dropdown. Uso: setticket opcao add <emoji> <nome>"""
    if not is_server_owner(ctx) and not ctx.author.guild_permissions.administrator:
        return await reply_and_delete(ctx, error_embed("Sem permissao.", ctx.guild))
    if not emoji or not nome:
        gd2 = get_guild_data(ctx.guild.id)
        p2 = gd2.get("prefix", PREFIX)
        return await reply_and_delete(ctx, error_embed(
            f"Uso correto: `{p2}setticket opcao add <emoji> <nome>`\nEx: `{p2}setticket opcao add 💀 duvidas`",
            ctx.guild
        ))
    gd = get_guild_data(ctx.guild.id)
    options = gd["ticket_config"].get("options", [])
    if len(options) >= 25:
        return await reply_and_delete(ctx, error_embed("Limite de 25 opcoes atingido.", ctx.guild))
    options.append({"emoji": emoji, "label": nome[:100]})
    gd["ticket_config"]["options"] = options
    update_guild_data(ctx.guild.id, gd)
    await reply_and_delete(ctx, success_embed("Opcao Adicionada", f"{emoji} **{nome}** adicionado ao dropdown.", ctx.guild))


@setticket_opcao.command(name="remove")
async def setticket_opcao_remove(ctx, numero: int = None):
    """Remove uma opcao pelo numero. Uso: setticket opcao remove <numero>"""
    if not is_server_owner(ctx) and not ctx.author.guild_permissions.administrator:
        return await reply_and_delete(ctx, error_embed("Sem permissao.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    options = gd["ticket_config"].get("options", [])
    if not numero or numero < 1 or numero > len(options):
        return await reply_and_delete(ctx, error_embed(f"Numero invalido. Ha {len(options)} opcoes.", ctx.guild))
    removed = options.pop(numero - 1)
    gd["ticket_config"]["options"] = options
    update_guild_data(ctx.guild.id, gd)
    await reply_and_delete(ctx, success_embed("Opcao Removida",
        f"{removed.get('emoji','')} **{removed.get('label','')}** removida.", ctx.guild))


@setticket_opcao.command(name="limpar")
async def setticket_opcao_limpar(ctx):
    """Remove todas as opcoes do dropdown (volta ao botao simples)."""
    if not is_server_owner(ctx) and not ctx.author.guild_permissions.administrator:
        return await reply_and_delete(ctx, error_embed("Sem permissao.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    gd["ticket_config"]["options"] = []
    update_guild_data(ctx.guild.id, gd)
    await reply_and_delete(ctx, success_embed("Opcoes Removidas", "Painel voltara a exibir botao simples.", ctx.guild))


@bot.command(name="criarticket")
async def criar_ticket_panel(ctx):
    """Envia o painel de abertura de tickets neste canal."""
    if not is_server_owner(ctx) and not ctx.author.guild_permissions.administrator:
        return await reply_and_delete(ctx, error_embed("Apenas o dono ou admins podem enviar o painel.", ctx.guild))
    if not ctx.guild:
        return

    gd = get_guild_data(ctx.guild.id)
    cfg = gd.get("ticket_config", {})
    color = get_guild_color(ctx.guild)

    embed = discord.Embed(
        title=cfg.get("title", "Suporte — Abrir Ticket"),
        description=cfg.get("description", "Clique abaixo para abrir um ticket."),
        color=color,
        timestamp=datetime.now(timezone.utc)
    )
    if ctx.guild.icon:
        embed.set_thumbnail(url=ctx.guild.icon.url)
    if ctx.guild.banner:
        embed.set_image(url=ctx.guild.banner.url)
    embed.set_footer(text=ctx.guild.name)

    try:
        await ctx.message.delete()
    except Exception:
        pass

    options = cfg.get("options", [])
    if options:
        view = TicketSelectView(options)
    else:
        view = TicketOpenView(ctx.guild.id)

    await ctx.channel.send(embed=embed, view=view)


@bot.command(name="addassumerole")
async def add_assume_role(ctx, cargo: discord.Role = None):
    """Adiciona um cargo com permissao de assumir tickets. Apenas dono do servidor."""
    if not is_server_owner(ctx) and not ctx.author.guild_permissions.administrator:
        return await reply_and_delete(ctx, error_embed("Apenas o dono do servidor ou admins podem configurar isso.", ctx.guild))
    if not cargo:
        return await reply_and_delete(ctx, error_embed("Mencione um cargo valido. Uso: `addassumerole @cargo`", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    assume_roles = gd["ticket_config"].get("assume_role_ids", [])
    if str(cargo.id) in assume_roles:
        return await reply_and_delete(ctx, error_embed(f"O cargo {cargo.mention} ja tem permissao de assumir tickets.", ctx.guild))
    assume_roles.append(str(cargo.id))
    gd["ticket_config"]["assume_role_ids"] = assume_roles
    update_guild_data(ctx.guild.id, gd)
    await reply_and_delete(ctx, success_embed(
        "Cargo Adicionado",
        f"Membros com o cargo {cargo.mention} agora podem assumir tickets e usar o painel.",
        ctx.guild
    ))


@bot.command(name="removeassumerole")
async def remove_assume_role(ctx, cargo: discord.Role = None):
    """Remove a permissao de assumir tickets de um cargo. Apenas dono do servidor."""
    if not is_server_owner(ctx) and not ctx.author.guild_permissions.administrator:
        return await reply_and_delete(ctx, error_embed("Apenas o dono do servidor ou admins podem configurar isso.", ctx.guild))
    if not cargo:
        return await reply_and_delete(ctx, error_embed("Mencione um cargo valido. Uso: `removeassumerole @cargo`", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    assume_roles = gd["ticket_config"].get("assume_role_ids", [])
    if str(cargo.id) not in assume_roles:
        return await reply_and_delete(ctx, error_embed(f"O cargo {cargo.mention} nao tem permissao de assumir tickets.", ctx.guild))
    assume_roles.remove(str(cargo.id))
    gd["ticket_config"]["assume_role_ids"] = assume_roles
    update_guild_data(ctx.guild.id, gd)
    await reply_and_delete(ctx, success_embed(
        "Cargo Removido",
        f"O cargo {cargo.mention} nao pode mais assumir tickets.",
        ctx.guild
    ))


@bot.command(name="assumeroles")
async def assume_roles_list(ctx):
    """Lista os cargos com permissao de assumir tickets."""
    if not is_server_owner(ctx) and not ctx.author.guild_permissions.administrator:
        return await reply_and_delete(ctx, error_embed("Sem permissao.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    assume_roles = gd["ticket_config"].get("assume_role_ids", [])
    embed = create_embed(ctx.guild, "Cargos com Permissao de Assumir Tickets")
    if not assume_roles:
        embed.description = "Nenhum cargo configurado. Apenas admins e o dono podem assumir tickets."
    else:
        lines = []
        for rid in assume_roles:
            role = ctx.guild.get_role(int(rid))
            lines.append(role.mention if role else f"~~Cargo removido ({rid})~~")
        embed.description = "\n".join(lines)
    await reply_and_delete(ctx, embed)


# ─── Boas-vindas ──────────────────────────────────────────────────────────────

@bot.command(name="boasvindas")
async def cmd_boasvindas(ctx, subcomando: str = None, *, valor: str = None):
    if not await check_activation(ctx):
        return
    gd = get_guild_data(ctx.guild.id)
    wc = gd.setdefault("welcome_config", {
        "enabled": False,
        "channel_id": None,
        "title": "Bem-vindo(a) ao servidor!",
        "description": "Olá, {user}! Seja bem-vindo(a) ao **{server}**!\nVocê é o membro de número **{count}**.",
        "color": None,
    })
    p = gd.get("prefix", PREFIX)

    # ── Sem subcomando: mostrar configuração ──
    if subcomando is None:
        status   = "Ativado" if wc.get("enabled") else "Desativado"
        canal_id = wc.get("channel_id")
        canal    = ctx.guild.get_channel(int(canal_id)).mention if canal_id and ctx.guild.get_channel(int(canal_id)) else "Nao definido"
        wc_color = wc.get("color")
        cor_str  = f"`#{wc_color.lstrip('#').upper()}`" if wc_color else "Padrao do servidor"
        embed = create_embed(ctx.guild, "Configuracao — Boas-vindas")
        embed.description = (
            f"**Status:** {status}\n"
            f"**Canal:** {canal}\n"
            f"**Cor:** {cor_str}\n"
            f"**Titulo:** {wc.get('title', '—')}\n"
            f"**Descricao:**\n{wc.get('description', '—')}\n\n"
            f"Variaveis disponiveis: `{{user}}` `{{server}}` `{{count}}`\n"
            f"Emojis do Discord funcionam normalmente no titulo e descricao.\n"
            f"A mensagem e deletada automaticamente apos **10 segundos**."
        )
        await reply_and_delete(ctx, embed)
        return

    sub = subcomando.lower()

    # ── ativar ──
    if sub == "ativar":
        if not wc.get("channel_id"):
            await reply_and_delete(ctx, error_embed(f"Defina o canal primeiro: `{p}boasvindas canal #canal`", ctx.guild))
            return
        wc["enabled"] = True
        update_guild_data(ctx.guild.id, gd)
        embed = create_embed(ctx.guild, "Boas-vindas Ativado")
        embed.description = "A mensagem de boas-vindas foi **ativada**. Novos membros receberao a mensagem ao entrar."
        await reply_and_delete(ctx, embed)
        return

    # ── desativar ──
    if sub == "desativar":
        wc["enabled"] = False
        update_guild_data(ctx.guild.id, gd)
        embed = create_embed(ctx.guild, "Boas-vindas Desativado")
        embed.description = "A mensagem de boas-vindas foi **desativada**."
        await reply_and_delete(ctx, embed)
        return

    # ── canal ──
    if sub == "canal":
        if not ctx.message.channel_mentions:
            await reply_and_delete(ctx, error_embed(f"Mencione um canal. Ex: `{p}boasvindas canal #geral`", ctx.guild))
            return
        ch = ctx.message.channel_mentions[0]
        wc["channel_id"] = str(ch.id)
        update_guild_data(ctx.guild.id, gd)
        embed = create_embed(ctx.guild, "Canal Definido")
        embed.description = f"Canal de boas-vindas definido para {ch.mention}."
        await reply_and_delete(ctx, embed)
        return

    # ── titulo ──
    if sub == "titulo":
        if not valor:
            await reply_and_delete(ctx, error_embed(f"Informe o titulo. Ex: `{p}boasvindas titulo Bem-vindo {{user}}!`", ctx.guild))
            return
        wc["title"] = valor
        update_guild_data(ctx.guild.id, gd)
        embed = create_embed(ctx.guild, "Titulo Atualizado")
        embed.description = f"Novo titulo:\n**{valor}**"
        await reply_and_delete(ctx, embed)
        return

    # ── descricao ──
    if sub in ("descricao", "descrição"):
        if not valor:
            await reply_and_delete(ctx, error_embed(f"Informe a descricao. Use `{{user}}`, `{{server}}`, `{{count}}`.", ctx.guild))
            return
        wc["description"] = valor
        update_guild_data(ctx.guild.id, gd)
        embed = create_embed(ctx.guild, "Descricao Atualizada")
        embed.description = f"Nova descricao:\n{valor}"
        await reply_and_delete(ctx, embed)
        return

    # ── cor ──
    if sub == "cor":
        if not valor:
            await reply_and_delete(ctx, error_embed(
                f"Informe a cor em hex. Ex: `{p}boasvindas cor #FF5733`\n"
                f"Para voltar ao padrao do servidor use: `{p}boasvindas cor reset`",
                ctx.guild
            ))
            return
        if valor.lower() == "reset":
            wc["color"] = None
            update_guild_data(ctx.guild.id, gd)
            embed = create_embed(ctx.guild, "Cor Redefinida")
            embed.description = "A cor das boas-vindas foi redefinida para o **padrao do servidor**."
            await reply_and_delete(ctx, embed)
            return
        hex_val = valor.lstrip("#")
        if len(hex_val) != 6:
            await reply_and_delete(ctx, error_embed(
                f"Cor invalida. Use um hex valido com 6 caracteres.\nEx: `{p}boasvindas cor #7C3AED`",
                ctx.guild
            ))
            return
        try:
            int(hex_val, 16)
        except ValueError:
            await reply_and_delete(ctx, error_embed(
                f"Cor invalida. Use um hex valido.\nEx: `{p}boasvindas cor #7C3AED`",
                ctx.guild
            ))
            return
        wc["color"] = hex_val.upper()
        update_guild_data(ctx.guild.id, gd)
        color_int = int(hex_val, 16)
        embed = discord.Embed(
            title="Cor das Boas-vindas Definida",
            description=f"A cor do embed de boas-vindas foi definida para `#{hex_val.upper()}`.\nEsta cor aparecera na proxima mensagem de boas-vindas.",
            color=color_int,
            timestamp=datetime.now(timezone.utc)
        )
        if ctx.guild.icon:
            embed.set_thumbnail(url=ctx.guild.icon.url)
        await reply_and_delete(ctx, embed)
        return

    await reply_and_delete(ctx, error_embed(
        f"Subcomando invalido. Use:\n"
        f"`{p}boasvindas canal #canal`\n"
        f"`{p}boasvindas ativar`\n"
        f"`{p}boasvindas desativar`\n"
        f"`{p}boasvindas cor <hex>` — ex: `#FF5733` ou `reset`\n"
        f"`{p}boasvindas titulo <texto>`\n"
        f"`{p}boasvindas descricao <texto>`",
        ctx.guild
    ))

# ═══════════════════════════════════════════════════════════════════════════════
# ─── NOVOS RECURSOS: Unmute / Uncastigo / MuteCall / AntiBan / Sorteio / IG ──
# ═══════════════════════════════════════════════════════════════════════════════

# ── helpers de permissão staff ────────────────────────────────────────────────

def _has_staff_perm(ctx) -> bool:
    """Retorna True se o autor tem cargo staff configurado, admin ou é dono."""
    if ctx.author.id == ctx.guild.owner_id:
        return True
    if ctx.author.guild_permissions.administrator:
        return True
    gd = get_guild_data(ctx.guild.id)
    staff_roles = gd.get("staff_roles", [])
    return any(str(r.id) in staff_roles for r in ctx.author.roles)

def _has_giveaway_perm(ctx) -> bool:
    if ctx.author.id == ctx.guild.owner_id:
        return True
    if ctx.author.guild_permissions.administrator:
        return True
    gd = get_guild_data(ctx.guild.id)
    gr = gd.get("giveaway_manager_roles", [])
    return any(str(r.id) in gr for r in ctx.author.roles)

def _has_instagram_perm(ctx) -> bool:
    if ctx.author.id == ctx.guild.owner_id:
        return True
    if ctx.author.guild_permissions.administrator:
        return True
    gd = get_guild_data(ctx.guild.id)
    ir = gd.get("instagram_roles", [])
    return any(str(r.id) in ir for r in ctx.author.roles)

# ── helpers de DB para punições ───────────────────────────────────────────────

def db_add_punishment(guild_id: int, user_id: int, ptype: str, role_id=None,
                      expires_at=None, moderator_id=None, reason=None):
    if not DATABASE_URL or not _PSYCOPG2_AVAILABLE:
        return
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        cur.execute("""
            UPDATE punishments SET active = FALSE
            WHERE guild_id=%s AND user_id=%s AND type=%s AND active=TRUE
        """, (str(guild_id), str(user_id), ptype))
        cur.execute("""
            INSERT INTO punishments (guild_id, user_id, type, role_id, expires_at, moderator_id, reason)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (str(guild_id), str(user_id), ptype,
              str(role_id) if role_id else None,
              expires_at, str(moderator_id) if moderator_id else None, reason))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[ERRO] db_add_punishment: {e}", flush=True)

def db_remove_punishment(guild_id: int, user_id: int, ptype: str):
    if not DATABASE_URL or not _PSYCOPG2_AVAILABLE:
        return None
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        cur.execute("""
            UPDATE punishments SET active=FALSE
            WHERE guild_id=%s AND user_id=%s AND type=%s AND active=TRUE
            RETURNING role_id
        """, (str(guild_id), str(user_id), ptype))
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return row["role_id"] if row else None
    except Exception as e:
        print(f"[ERRO] db_remove_punishment: {e}", flush=True)
        return None

def db_get_expired_punishments():
    if not DATABASE_URL or not _PSYCOPG2_AVAILABLE:
        return []
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, guild_id, user_id, type, role_id
            FROM punishments
            WHERE active=TRUE AND expires_at IS NOT NULL AND expires_at <= NOW()
        """)
        rows = list(cur.fetchall())
        conn.commit()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        print(f"[ERRO] db_get_expired_punishments: {e}", flush=True)
        return []

def db_deactivate_punishment(pid: int):
    if not DATABASE_URL or not _PSYCOPG2_AVAILABLE:
        return
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        cur.execute("UPDATE punishments SET active=FALSE WHERE id=%s", (pid,))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[ERRO] db_deactivate_punishment: {e}", flush=True)

# ── helpers de DB para sorteios ───────────────────────────────────────────────

def db_create_giveaway(guild_id, channel_id, host_id, prize, winners_count, ends_at):
    if not DATABASE_URL or not _PSYCOPG2_AVAILABLE:
        return None
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO giveaways (guild_id, channel_id, host_id, prize, winners_count, ends_at)
            VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
        """, (str(guild_id), str(channel_id), str(host_id), prize, winners_count, ends_at))
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return row["id"] if row else None
    except Exception as e:
        print(f"[ERRO] db_create_giveaway: {e}", flush=True)
        return None

def db_update_giveaway_message(giveaway_id, message_id):
    if not DATABASE_URL or not _PSYCOPG2_AVAILABLE:
        return
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        cur.execute("UPDATE giveaways SET message_id=%s WHERE id=%s", (str(message_id), giveaway_id))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[ERRO] db_update_giveaway_message: {e}", flush=True)

def db_get_active_giveaways():
    if not DATABASE_URL or not _PSYCOPG2_AVAILABLE:
        return []
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        cur.execute("SELECT * FROM giveaways WHERE ended=FALSE AND ends_at <= NOW()")
        rows = list(cur.fetchall())
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        print(f"[ERRO] db_get_active_giveaways: {e}", flush=True)
        return []

def db_end_giveaway(giveaway_id, winners):
    if not DATABASE_URL or not _PSYCOPG2_AVAILABLE:
        return
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        cur.execute("""
            UPDATE giveaways SET ended=TRUE, winners=%s WHERE id=%s
        """, (winners, giveaway_id))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[ERRO] db_end_giveaway: {e}", flush=True)

def db_add_giveaway_entry(giveaway_id, user_id):
    if not DATABASE_URL or not _PSYCOPG2_AVAILABLE:
        return
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        cur.execute("""
            UPDATE giveaways SET entries = array_append(entries, %s)
            WHERE id=%s AND NOT (%s = ANY(entries))
        """, (str(user_id), giveaway_id, str(user_id)))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[ERRO] db_add_giveaway_entry: {e}", flush=True)

# ── helpers de DB para posts Instagram ───────────────────────────────────────

def db_add_post(guild_id, channel_id, author_id, image_url, caption=None, message_id=None):
    if not DATABASE_URL or not _PSYCOPG2_AVAILABLE:
        return
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO posts (guild_id, channel_id, author_id, image_url, caption, message_id)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (str(guild_id), str(channel_id), str(author_id), image_url, caption,
              str(message_id) if message_id else None))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[ERRO] db_add_post: {e}", flush=True)

# ── Unmute ────────────────────────────────────────────────────────────────────

@bot.command(name="unmute")
@commands.has_permissions(moderate_members=True)
async def unmute(ctx, member: discord.Member = None):
    if not member:
        return await reply_and_delete(ctx, error_embed("Mencione um usuário válido.", ctx.guild))
    try:
        _pending_bot_actions.add((ctx.guild.id, member.id, "unmute"))
        await member.timeout(None)
    except discord.Forbidden:
        _pending_bot_actions.discard((ctx.guild.id, member.id, "unmute"))
        return await reply_and_delete(ctx, error_embed("Sem permissão para remover mute.", ctx.guild))
    embed = create_embed(ctx.guild, "Mute Removido")
    embed.description = (
        f"**Usuário:** {member.mention} ({member.id})\n"
        f"**Moderador:** {ctx.author.mention} ({ctx.author.id})\n"
        f"**Horário:** <t:{int(datetime.now(timezone.utc).timestamp())}:F>"
    )
    await send_log(ctx.guild, "mute", embed)
    await reply_and_delete(ctx, success_embed("Unmute", f"{member} teve o mute removido.", ctx.guild))

# ── Uncastigo ─────────────────────────────────────────────────────────────────

@bot.command(name="uncastigo")
@commands.has_permissions(manage_roles=True)
async def uncastigo(ctx, member: discord.Member = None):
    if not member:
        return await reply_and_delete(ctx, error_embed("Mencione um usuário válido.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    role_id = gd.get("castigo_role_id")
    castigo_role = None
    if role_id:
        castigo_role = ctx.guild.get_role(int(role_id))
    if not castigo_role:
        castigo_role = discord.utils.get(ctx.guild.roles, name="castigo")
    if not castigo_role or castigo_role not in member.roles:
        return await reply_and_delete(ctx, error_embed("Este membro não está em castigo.", ctx.guild))
    try:
        _pending_bot_actions.add((ctx.guild.id, member.id, "uncastigo"))
        await member.remove_roles(castigo_role, reason=f"Castigo removido por {ctx.author}")
    except discord.Forbidden:
        _pending_bot_actions.discard((ctx.guild.id, member.id, "uncastigo"))
        return await reply_and_delete(ctx, error_embed("Sem permissão para remover cargos.", ctx.guild))
    db_remove_punishment(ctx.guild.id, member.id, "castigo")
    embed = create_embed(ctx.guild, "Castigo Removido")
    embed.description = (
        f"**Usuário:** {member.mention} ({member.id})\n"
        f"**Moderador:** {ctx.author.mention} ({ctx.author.id})\n"
        f"**Horário:** <t:{int(datetime.now(timezone.utc).timestamp())}:F>"
    )
    await send_log(ctx.guild, "castigo", embed)
    await reply_and_delete(ctx, success_embed("Uncastigo", f"{member} teve o castigo removido.", ctx.guild))

# ── MuteCall ──────────────────────────────────────────────────────────────────

@bot.command(name="mutecall")
@commands.has_permissions(mute_members=True)
async def mutecall(ctx, member: discord.Member = None, *, reason="Sem motivo informado"):
    if not member:
        return await reply_and_delete(ctx, error_embed("Mencione um usuário válido.", ctx.guild))
    if is_protected(ctx.guild.id, member.id):
        return await reply_and_delete(ctx, error_embed("Este membro está protegido.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    role_id = gd.get("mute_call_role_id")
    mute_role = ctx.guild.get_role(int(role_id)) if role_id else None
    if mute_role:
        try:
            _pending_bot_actions.add((ctx.guild.id, member.id, "mutecall"))
            await member.add_roles(mute_role, reason=reason)
        except discord.Forbidden:
            _pending_bot_actions.discard((ctx.guild.id, member.id, "mutecall"))
            return await reply_and_delete(ctx, error_embed("Sem permissão para adicionar cargos.", ctx.guild))
    else:
        # Mute direto no Discord
        try:
            _pending_bot_actions.add((ctx.guild.id, member.id, "mutecall"))
            if member.voice:
                await member.edit(mute=True, reason=reason)
        except discord.Forbidden:
            _pending_bot_actions.discard((ctx.guild.id, member.id, "mutecall"))
            return await reply_and_delete(ctx, error_embed("Sem permissão para mutar no canal de voz.", ctx.guild))
    db_add_punishment(ctx.guild.id, member.id, "mutecall",
                      role_id=role_id, moderator_id=ctx.author.id, reason=reason)
    embed = create_embed(ctx.guild, "Mute de Voz Aplicado")
    embed.description = (
        f"**Usuário:** {member.mention} ({member.id})\n"
        f"**Moderador:** {ctx.author.mention} ({ctx.author.id})\n"
        f"**Motivo:** {reason}\n"
        f"**Horário:** <t:{int(datetime.now(timezone.utc).timestamp())}:F>"
    )
    await send_log(ctx.guild, "mute", embed)
    await reply_and_delete(ctx, success_embed("MuteCall", f"{member} foi mutado no canal de voz.\nMotivo: {reason}", ctx.guild))

@bot.command(name="unmutecall")
@commands.has_permissions(mute_members=True)
async def unmutecall(ctx, member: discord.Member = None):
    if not member:
        return await reply_and_delete(ctx, error_embed("Mencione um usuário válido.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    role_id = gd.get("mute_call_role_id")
    mute_role = ctx.guild.get_role(int(role_id)) if role_id else None
    removed = False
    if mute_role and mute_role in member.roles:
        try:
            await member.remove_roles(mute_role, reason=f"MuteCall removido por {ctx.author}")
            removed = True
        except discord.Forbidden:
            return await reply_and_delete(ctx, error_embed("Sem permissão para remover cargos.", ctx.guild))
    if not removed and member.voice:
        try:
            await member.edit(mute=False, reason=f"UnmuteCall por {ctx.author}")
            removed = True
        except discord.Forbidden:
            pass
    db_remove_punishment(ctx.guild.id, member.id, "mutecall")
    await reply_and_delete(ctx, success_embed("UnmuteCall", f"{member} teve o mute de voz removido.", ctx.guild))

# ── AntibanRole ───────────────────────────────────────────────────────────────

# (addantban / removeantban já existem acima — aqui adicionamos o setantibanrole)
# Os comandos addantban/removeantban do original usam "anti_ban_users" no JSON,
# o setantibanrole define qual cargo é dado automaticamente ao entrar.

# ── Sorteio ───────────────────────────────────────────────────────────────────

import random as _random

class SorteioModal(discord.ui.Modal, title="Criar Sorteio"):
    premio = discord.ui.TextInput(
        label="Prêmio",
        placeholder="Ex: Nitro, R$50, etc.",
        max_length=100,
    )
    duracao = discord.ui.TextInput(
        label="Duração",
        placeholder="Ex: 10m, 1h, 1d, 7d",
        max_length=10,
    )
    ganhadores = discord.ui.TextInput(
        label="Número de ganhadores",
        placeholder="Ex: 1",
        max_length=3,
        default="1",
    )

    def __init__(self, guild, channel, host):
        super().__init__()
        self.guild = guild
        self.channel = channel
        self.host = host

    async def on_submit(self, interaction: discord.Interaction):
        units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
        raw = self.duracao.value.strip().lower()
        unit = raw[-1] if raw else ""
        if unit not in units or not raw[:-1].isdigit():
            return await interaction.response.send_message(
                "❌ Duração inválida. Use: `10m`, `1h`, `1d`, `7d`", ephemeral=True)
        seconds = int(raw[:-1]) * units[unit]
        try:
            winners_count = max(1, int(self.ganhadores.value.strip()))
        except ValueError:
            winners_count = 1
        ends_at = datetime.now(timezone.utc) + timedelta(seconds=seconds)
        prize = self.premio.value.strip()

        gaw_id = db_create_giveaway(self.guild.id, self.channel.id,
                                    self.host.id, prize, winners_count, ends_at)

        embed = discord.Embed(
            title="🎉 SORTEIO 🎉",
            description=(
                f"**Prêmio:** {prize}\n"
                f"**Ganhadores:** {winners_count}\n"
                f"**Encerra:** <t:{int(ends_at.timestamp())}:R>\n"
                f"**Organizador:** {self.host.mention}\n\n"
                f"Reaja com 🎉 para participar!"
            ),
            color=EMBED_COLOR,
        )
        embed.set_footer(text=f"ID: {gaw_id}" if gaw_id else "Sorteio")
        await interaction.response.defer()
        msg = await self.channel.send(embed=embed)
        await msg.add_reaction("🎉")
        if gaw_id:
            db_update_giveaway_message(gaw_id, msg.id)


@bot.command(name="sorteio")
async def sorteio(ctx):
    if not _has_giveaway_perm(ctx):
        return await reply_and_delete(ctx, error_embed(
            "Você não tem permissão para criar sorteios.", ctx.guild))
    try:
        await ctx.message.delete()
    except Exception:
        pass
    modal = SorteioModal(ctx.guild, ctx.channel, ctx.author)
    # Como não podemos abrir modal via command prefix, enviamos painel intermediário
    class _OpenModal(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=30)
        @discord.ui.button(label="Criar Sorteio 🎉", style=discord.ButtonStyle.success)
        async def open(self, interaction: discord.Interaction, button: discord.ui.Button):
            if interaction.user.id != ctx.author.id:
                return await interaction.response.send_message("Apenas quem pediu pode usar.", ephemeral=True)
            await interaction.response.send_modal(
                SorteioModal(ctx.guild, ctx.channel, ctx.author))
            self.stop()
    view = _OpenModal()
    msg = await ctx.channel.send(
        embed=create_embed(ctx.guild, "Criar Sorteio", "Clique no botão para configurar o sorteio."),
        view=view)
    await view.wait()
    try:
        await msg.delete()
    except Exception:
        pass

# ── Instagram ─────────────────────────────────────────────────────────────────

class ComentarioModal(discord.ui.Modal, title="Adicionar Comentário"):
    comentario = discord.ui.TextInput(
        label="Comentário",
        style=discord.TextStyle.paragraph,
        placeholder="Escreva seu comentário...",
        max_length=300,
        required=False,
    )

    def __init__(self, post_msg):
        super().__init__()
        self.post_msg = post_msg

    async def on_submit(self, interaction: discord.Interaction):
        text = self.comentario.value.strip()
        if not text:
            return await interaction.response.send_message("Comentário vazio.", ephemeral=True)
        embed = discord.Embed(
            description=f"💬 **{interaction.user.display_name}:** {text}",
            color=0x9b59b6,
        )
        embed.set_thumbnail(url=interaction.user.display_avatar.url)
        await interaction.response.send_message(embed=embed)


class InstagramView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="❤️ Curtir", style=discord.ButtonStyle.danger, custom_id="ig_like")
    async def curtir(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            f"❤️ **{interaction.user.display_name}** curtiu esta foto!", ephemeral=False)

    @discord.ui.button(label="💬 Comentar", style=discord.ButtonStyle.secondary, custom_id="ig_comment")
    async def comentar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ComentarioModal(interaction.message))

    @discord.ui.button(label="📤 Compartilhar", style=discord.ButtonStyle.primary, custom_id="ig_share")
    async def compartilhar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            f"📤 **{interaction.user.display_name}** compartilhou esta foto!", ephemeral=False)


@bot.command(name="instagram")
async def instagram_post(ctx, *, caption: str = ""):
    if not _has_instagram_perm(ctx):
        return await reply_and_delete(ctx, error_embed(
            "Você não tem permissão para postar no Instagram.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    ch_id = gd.get("instagram_channel_id")
    channel = ctx.guild.get_channel(int(ch_id)) if ch_id else ctx.channel

    if not ctx.message.attachments:
        return await reply_and_delete(ctx, error_embed(
            "Anexe uma imagem ao comando.", ctx.guild))

    image_url = ctx.message.attachments[0].url
    embed = discord.Embed(
        description=caption if caption else None,
        color=0xC13584,
    )
    embed.set_author(name=ctx.author.display_name, icon_url=ctx.author.display_avatar.url)
    embed.set_image(url=image_url)
    embed.set_footer(text="yov! Instagram")
    embed.timestamp = datetime.now(timezone.utc)

    try:
        await ctx.message.delete()
    except Exception:
        pass

    view = InstagramView()
    msg = await channel.send(embed=embed, view=view)
    db_add_post(ctx.guild.id, channel.id, ctx.author.id, image_url, caption, msg.id)

# ── Comandos de Configuração dos Novos Campos ─────────────────────────────────

@bot.command(name="setstaffrole")
async def setstaffrole(ctx, acao: str = None, cargo: discord.Role = None):
    if ctx.author.id != ctx.guild.owner_id and not ctx.author.guild_permissions.administrator:
        return await reply_and_delete(ctx, error_embed("Apenas admin ou dono.", ctx.guild))
    if not acao or acao not in ("add", "remove", "lista"):
        p = await get_prefix(bot, ctx.message)
        return await reply_and_delete(ctx, error_embed(
            f"Use: `{p}setstaffrole add @cargo` / `remove @cargo` / `lista`", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    if acao == "lista":
        roles = gd.get("staff_roles", [])
        if not roles:
            return await reply_and_delete(ctx, create_embed(ctx.guild, "Staff Roles", "Nenhum cargo staff configurado."))
        names = [f"<@&{r}>" for r in roles]
        return await reply_and_delete(ctx, create_embed(ctx.guild, "Staff Roles", "\n".join(names)))
    if not cargo:
        return await reply_and_delete(ctx, error_embed("Mencione um cargo.", ctx.guild))
    sr = gd.setdefault("staff_roles", [])
    if acao == "add":
        if str(cargo.id) not in sr:
            sr.append(str(cargo.id))
        update_guild_data(ctx.guild.id, gd)
        await reply_and_delete(ctx, success_embed("Staff Role", f"{cargo.mention} adicionado como staff.", ctx.guild))
    elif acao == "remove":
        try:
            sr.remove(str(cargo.id))
        except ValueError:
            return await reply_and_delete(ctx, error_embed("Cargo não está na lista.", ctx.guild))
        update_guild_data(ctx.guild.id, gd)
        await reply_and_delete(ctx, success_embed("Staff Role", f"{cargo.mention} removido.", ctx.guild))

def _make_role_setter(field: str, label: str):
    """Fabrica um comando setXXXrole para campos simples de role_id."""
    async def _cmd(ctx, cargo: discord.Role = None):
        if ctx.author.id != ctx.guild.owner_id and not ctx.author.guild_permissions.administrator:
            return await reply_and_delete(ctx, error_embed("Apenas admin ou dono.", ctx.guild))
        gd = get_guild_data(ctx.guild.id)
        if cargo is None:
            gd[field] = None
            update_guild_data(ctx.guild.id, gd)
            return await reply_and_delete(ctx, success_embed(label, f"{label} removido.", ctx.guild))
        gd[field] = str(cargo.id)
        update_guild_data(ctx.guild.id, gd)
        await reply_and_delete(ctx, success_embed(label, f"{cargo.mention} definido como {label}.", ctx.guild))
    return _cmd

bot.command(name="setmuterole")(_make_role_setter("mute_role_id", "Cargo Mute"))
bot.command(name="setcastigorole")(_make_role_setter("castigo_role_id", "Cargo Castigo"))
bot.command(name="setmutecallrole")(_make_role_setter("mute_call_role_id", "Cargo MuteCall"))
bot.command(name="setantibanrole")(_make_role_setter("antiban_role_id", "Cargo AntiBan"))

@bot.command(name="setinstagramcanal")
async def setinstagramcanal(ctx, channel: discord.TextChannel = None):
    if ctx.author.id != ctx.guild.owner_id and not ctx.author.guild_permissions.administrator:
        return await reply_and_delete(ctx, error_embed("Apenas admin ou dono.", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    if channel is None:
        gd["instagram_channel_id"] = None
        update_guild_data(ctx.guild.id, gd)
        return await reply_and_delete(ctx, success_embed("Instagram", "Canal de Instagram removido.", ctx.guild))
    gd["instagram_channel_id"] = str(channel.id)
    update_guild_data(ctx.guild.id, gd)
    await reply_and_delete(ctx, success_embed("Instagram", f"Canal de posts definido: {channel.mention}", ctx.guild))

@bot.command(name="setinstagramrole")
async def setinstagramrole(ctx, acao: str = None, cargo: discord.Role = None):
    if ctx.author.id != ctx.guild.owner_id and not ctx.author.guild_permissions.administrator:
        return await reply_and_delete(ctx, error_embed("Apenas admin ou dono.", ctx.guild))
    if not acao or acao not in ("add", "remove", "lista"):
        p = await get_prefix(bot, ctx.message)
        return await reply_and_delete(ctx, error_embed(
            f"Use: `{p}setinstagramrole add @cargo` / `remove @cargo` / `lista`", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    if acao == "lista":
        roles = gd.get("instagram_roles", [])
        names = [f"<@&{r}>" for r in roles] if roles else ["Nenhum"]
        return await reply_and_delete(ctx, create_embed(ctx.guild, "Instagram Roles", "\n".join(names)))
    if not cargo:
        return await reply_and_delete(ctx, error_embed("Mencione um cargo.", ctx.guild))
    ir = gd.setdefault("instagram_roles", [])
    if acao == "add":
        if str(cargo.id) not in ir:
            ir.append(str(cargo.id))
        update_guild_data(ctx.guild.id, gd)
        await reply_and_delete(ctx, success_embed("Instagram Role", f"{cargo.mention} pode postar no Instagram.", ctx.guild))
    elif acao == "remove":
        try:
            ir.remove(str(cargo.id))
        except ValueError:
            return await reply_and_delete(ctx, error_embed("Cargo não está na lista.", ctx.guild))
        update_guild_data(ctx.guild.id, gd)
        await reply_and_delete(ctx, success_embed("Instagram Role", f"{cargo.mention} removido.", ctx.guild))

@bot.command(name="setsorteiorole")
async def setsorteiorole(ctx, acao: str = None, cargo: discord.Role = None):
    if ctx.author.id != ctx.guild.owner_id and not ctx.author.guild_permissions.administrator:
        return await reply_and_delete(ctx, error_embed("Apenas admin ou dono.", ctx.guild))
    if not acao or acao not in ("add", "remove", "lista"):
        p = await get_prefix(bot, ctx.message)
        return await reply_and_delete(ctx, error_embed(
            f"Use: `{p}setsorteiorole add @cargo` / `remove @cargo` / `lista`", ctx.guild))
    gd = get_guild_data(ctx.guild.id)
    if acao == "lista":
        roles = gd.get("giveaway_manager_roles", [])
        names = [f"<@&{r}>" for r in roles] if roles else ["Nenhum"]
        return await reply_and_delete(ctx, create_embed(ctx.guild, "Sorteio Roles", "\n".join(names)))
    if not cargo:
        return await reply_and_delete(ctx, error_embed("Mencione um cargo.", ctx.guild))
    gr = gd.setdefault("giveaway_manager_roles", [])
    if acao == "add":
        if str(cargo.id) not in gr:
            gr.append(str(cargo.id))
        update_guild_data(ctx.guild.id, gd)
        await reply_and_delete(ctx, success_embed("Sorteio Role", f"{cargo.mention} pode criar sorteios.", ctx.guild))
    elif acao == "remove":
        try:
            gr.remove(str(cargo.id))
        except ValueError:
            return await reply_and_delete(ctx, error_embed("Cargo não está na lista.", ctx.guild))
        update_guild_data(ctx.guild.id, gd)
        await reply_and_delete(ctx, success_embed("Sorteio Role", f"{cargo.mention} removido.", ctx.guild))

# ── On Member Join — conceder antiban_role se configurado ────────────────────
# (o on_member_join original já existe; vamos adicionar hook via setup_hook)

_original_on_member_join = None

# ── Tasks ─────────────────────────────────────────────────────────────────────

from discord.ext import tasks as _tasks

@_tasks.loop(seconds=30)
async def check_punishments_task():
    """Remove punições expiradas (castigo/mutecall por role)."""
    expired = db_get_expired_punishments()
    for row in expired:
        try:
            guild = bot.get_guild(int(row["guild_id"]))
            if not guild:
                db_deactivate_punishment(row["id"])
                continue
            member = guild.get_member(int(row["user_id"]))
            if not member:
                db_deactivate_punishment(row["id"])
                continue
            role_id = row["role_id"]
            if role_id:
                role = guild.get_role(int(role_id))
                if role and role in member.roles:
                    try:
                        await member.remove_roles(role, reason="Punição expirada")
                    except Exception:
                        pass
            db_deactivate_punishment(row["id"])
        except Exception as e:
            print(f"[TASK] check_punishments_task: {e}", flush=True)

@_tasks.loop(seconds=30)
async def check_giveaways_task():
    """Encerra sorteios que passaram do prazo."""
    ended = db_get_active_giveaways()
    for row in ended:
        try:
            guild = bot.get_guild(int(row["guild_id"]))
            if not guild:
                db_end_giveaway(row["id"], [])
                continue
            channel = guild.get_channel(int(row["channel_id"]))
            entries = list(row.get("entries") or [])
            winners_count = row.get("winners_count", 1)
            prize = row.get("prize", "Prêmio")
            winners = []
            if entries:
                sample = min(winners_count, len(entries))
                winners = _random.sample(entries, sample)

            db_end_giveaway(row["id"], winners)

            if channel:
                if winners:
                    mentions = " ".join(f"<@{w}>" for w in winners)
                    embed = discord.Embed(
                        title="🎉 Sorteio Encerrado!",
                        description=(
                            f"**Prêmio:** {prize}\n"
                            f"**Vencedor(es):** {mentions}\n\n"
                            f"Parabéns!"
                        ),
                        color=EMBED_COLOR,
                    )
                else:
                    embed = discord.Embed(
                        title="🎉 Sorteio Encerrado",
                        description=f"**Prêmio:** {prize}\n\nNinguém participou.",
                        color=EMBED_COLOR,
                    )
                await channel.send(embed=embed)

                # Tenta atualizar mensagem original
                if row.get("message_id"):
                    try:
                        msg = await channel.fetch_message(int(row["message_id"]))
                        e2 = msg.embeds[0] if msg.embeds else discord.Embed()
                        e2.title = "🎉 SORTEIO ENCERRADO 🎉"
                        e2.color = 0x555555
                        await msg.edit(embed=e2)
                    except Exception:
                        pass
        except Exception as e:
            print(f"[TASK] check_giveaways_task: {e}", flush=True)

@bot.event
async def on_member_join_antiban_hook(member: discord.Member):
    """Dá o cargo antiban_role_id ao membro ao entrar, se configurado."""
    gd = get_guild_data(member.guild.id)
    role_id = gd.get("antiban_role_id")
    if role_id:
        role = member.guild.get_role(int(role_id))
        if role:
            try:
                await member.add_roles(role, reason="Cargo automático anti-ban")
            except Exception:
                pass

# Registra o hook extra após o bot estar pronto
@bot.listen("on_member_join")
async def _member_join_antiban(member: discord.Member):
    await on_member_join_antiban_hook(member)

# Inicia as tasks quando o bot ficar pronto
@bot.listen("on_ready")
async def _start_tasks():
    if not check_punishments_task.is_running():
        check_punishments_task.start()
    if not check_giveaways_task.is_running():
        check_giveaways_task.start()

# ─── Error Handler ────────────────────────────────────────────────────────────

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandOnCooldown):
        await reply_and_delete(ctx, error_embed(f"Aguarde {error.retry_after:.1f}s.", ctx.guild))
    elif isinstance(error, commands.MissingPermissions):
        await reply_and_delete(ctx, error_embed("Voce nao tem permissao para usar este comando.", ctx.guild))
    elif isinstance(error, commands.BotMissingPermissions):
        await reply_and_delete(ctx, error_embed("Eu nao tenho permissoes suficientes para esta acao.", ctx.guild))
    elif isinstance(error, (commands.MemberNotFound, commands.UserNotFound)):
        await reply_and_delete(ctx, error_embed("Usuario nao encontrado.", ctx.guild))
    elif isinstance(error, commands.RoleNotFound):
        await reply_and_delete(ctx, error_embed("Cargo nao encontrado.", ctx.guild))
    elif isinstance(error, commands.ChannelNotFound):
        await reply_and_delete(ctx, error_embed("Canal nao encontrado.", ctx.guild))
    elif isinstance(error, (commands.BadArgument, commands.MissingRequiredArgument)):
        p = await get_prefix(bot, ctx.message)
        await reply_and_delete(ctx, error_embed(f"Argumento invalido. Verifique com {p}help.", ctx.guild))
    elif isinstance(error, commands.CommandNotFound):
        pass
    elif isinstance(error, commands.CheckFailure):
        pass  # Silencioso — check_activation ja envia a mensagem
    elif isinstance(error, discord.Forbidden):
        await reply_and_delete(ctx, error_embed("Nao tenho permissoes suficientes para esta acao.", ctx.guild))
    elif isinstance(error, discord.HTTPException):
        print(f"[ERRO HTTP] {error}", flush=True)
    else:
        print(f"[ERRO] Comando '{ctx.command}': {type(error).__name__}: {error}", flush=True)
        traceback.print_exc()

# ─── Iniciar o Bot ────────────────────────────────────────────────────────────

if not TOKEN:
    print("[ERRO FATAL] DISCORD_TOKEN nao definido.", flush=True)
    sys.exit(1)

# Inicializa o banco de dados PostgreSQL (se DATABASE_URL estiver definida)
init_db_postgres()

try:
    bot.run(TOKEN, log_handler=None)
except discord.LoginFailure:
    print("[ERRO FATAL] Token invalido.", flush=True)
    sys.exit(1)
except discord.PrivilegedIntentsRequired:
    print("[ERRO FATAL] Intents privilegiadas nao ativadas no Developer Portal.", flush=True)
    sys.exit(1)
except KeyboardInterrupt:
    print("[BOT] Encerrado.", flush=True)
except Exception as e:
    print(f"[ERRO FATAL] {type(e).__name__}: {e}", flush=True)
    traceback.print_exc()
    sys.exit(1)
