import os
import time
import random
import asyncio
import sqlite3
import logging
import hashlib
from datetime import datetime, timezone
from typing import Optional

from aiohttp import web
from dotenv import load_dotenv
import discord
from discord.ext import commands, tasks

# ============================================================
# GLOBAL ECONOMY + LEVELING BOT
# GitHub: main.py | Render: Python service
# ============================================================

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
DB_PATH = os.getenv("DB_PATH", "economy.db")
PORT = int(os.getenv("PORT", "10000"))
PREFIX = "$"

# Economy settings
DAILY_REWARD = 1000
WORK_MIN = 300
WORK_MAX = 1000
DAILY_COOLDOWN = 24 * 60 * 60
WORK_COOLDOWN = 6 * 60 * 60
GAME_COOLDOWN = 3

# XP settings
XP_COOLDOWN = 10                 # one valid XP message per user every 10 sec
REPEAT_MESSAGE_COOLDOWN = 60     # same message cannot farm XP
MIN_MESSAGE_LENGTH = 3

# Live leaderboard / presence
BALTOP_REFRESH = 45
STATUS_REFRESH = 25

# Level rewards: L1=100, L2=150, L3=200...
LEVEL_REWARD_BASE = 100
LEVEL_REWARD_STEP = 50

# Requested milestones. These are cumulative valid-message milestones.
LEVEL_MILESTONES = {1: 20, 2: 40, 3: 100}

# Slots
SLOT_SYMBOLS = ["🍒", "⭐", "💎", "🍀", "7️⃣"]
SLOT_WEIGHTS = [34, 28, 18, 14, 6]
SLOT_TRIPLE_MULTIPLIER = {
    "🍒": 4,
    "🍀": 5,
    "⭐": 6,
    "💎": 8,
    "7️⃣": 10,
}

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("economy-bot")

# ============================================================
# DISCORD
# ============================================================

intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True

bot = commands.Bot(
    command_prefix=PREFIX,
    intents=intents,
    case_insensitive=True,
    strip_after_prefix=True,
    help_command=None,
)

# ============================================================
# SQLITE
# IMPORTANT: users are keyed ONLY by user_id, never guild_id.
# This makes the economy global across all servers.
# ============================================================

os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
db = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
db.row_factory = sqlite3.Row
db.execute("PRAGMA journal_mode=WAL")
db.execute("PRAGMA synchronous=NORMAL")
db.execute("PRAGMA foreign_keys=ON")
db_lock = asyncio.Lock()


def init_db():
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            coins INTEGER NOT NULL DEFAULT 0,
            bank INTEGER NOT NULL DEFAULT 0,
            level INTEGER NOT NULL DEFAULT 0,
            level_messages INTEGER NOT NULL DEFAULT 0,
            total_messages INTEGER NOT NULL DEFAULT 0,
            daily_last INTEGER NOT NULL DEFAULT 0,
            work_last INTEGER NOT NULL DEFAULT 0,
            xp_last INTEGER NOT NULL DEFAULT 0,
            last_message_hash TEXT NOT NULL DEFAULT '',
            last_message_at INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            amount INTEGER NOT NULL,
            related_user_id INTEGER,
            balance_after INTEGER NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_tx_user ON transactions(user_id, created_at DESC);

        CREATE TABLE IF NOT EXISTS baltop_config (
            guild_id INTEGER PRIMARY KEY,
            channel_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS level_config (
            guild_id INTEGER PRIMARY KEY,
            channel_id INTEGER,
            enabled INTEGER NOT NULL DEFAULT 1
        );
        """
    )


init_db()
# Backward-compatible migration for existing economy.db files.
try:
    db.execute("ALTER TABLE users ADD COLUMN bank INTEGER NOT NULL DEFAULT 0")
except sqlite3.OperationalError:
    pass

# ============================================================
# DATABASE HELPERS
# ============================================================

async def ensure_user(user_id: int):
    async with db_lock:
        db.execute(
            """
            INSERT OR IGNORE INTO users
            (user_id, created_at) VALUES (?, ?)
            """,
            (user_id, int(time.time())),
        )


async def get_user(user_id: int):
    await ensure_user(user_id)
    async with db_lock:
        return db.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()


async def add_tx(user_id: int, tx_type: str, amount: int, balance_after: int,
                 description: str = "", related_user_id: Optional[int] = None):
    db.execute(
        """
        INSERT INTO transactions
        (user_id,type,amount,related_user_id,balance_after,description,created_at)
        VALUES (?,?,?,?,?,?,?)
        """,
        (user_id, tx_type, amount, related_user_id, balance_after,
         description[:250], int(time.time())),
    )


async def change_balance(user_id: int, delta: int, tx_type: str,
                         description: str = "", related_user_id: Optional[int] = None):
    """Atomic global balance update. Returns (success, new_balance)."""
    await ensure_user(user_id)
    async with db_lock:
        db.execute("BEGIN IMMEDIATE")
        try:
            row = db.execute("SELECT coins FROM users WHERE user_id=?", (user_id,)).fetchone()
            old = int(row["coins"])
            new = old + int(delta)
            if new < 0:
                db.execute("ROLLBACK")
                return False, old
            db.execute("UPDATE users SET coins=? WHERE user_id=?", (new, user_id))
            awaitable = False  # keeps this function entirely synchronous inside lock
            db.execute(
                """
                INSERT INTO transactions
                (user_id,type,amount,related_user_id,balance_after,description,created_at)
                VALUES (?,?,?,?,?,?,?)
                """,
                (user_id, tx_type, int(delta), related_user_id, new,
                 description[:250], int(time.time())),
            )
            db.execute("COMMIT")
            return True, new
        except Exception:
            db.execute("ROLLBACK")
            raise


async def transfer(sender_id: int, receiver_id: int, amount: int):
    if sender_id == receiver_id:
        return False, "self", 0, 0
    if amount <= 0:
        return False, "amount", 0, 0
    await ensure_user(sender_id)
    await ensure_user(receiver_id)

    async with db_lock:
        db.execute("BEGIN IMMEDIATE")
        try:
            s = db.execute("SELECT coins FROM users WHERE user_id=?", (sender_id,)).fetchone()
            r = db.execute("SELECT coins FROM users WHERE user_id=?", (receiver_id,)).fetchone()
            sb = int(s["coins"])
            rb = int(r["coins"])
            if sb < amount:
                db.execute("ROLLBACK")
                return False, "insufficient", sb, rb
            ns, nr = sb - amount, rb + amount
            now = int(time.time())
            db.execute("UPDATE users SET coins=? WHERE user_id=?", (ns, sender_id))
            db.execute("UPDATE users SET coins=? WHERE user_id=?", (nr, receiver_id))
            db.execute(
                """INSERT INTO transactions
                (user_id,type,amount,related_user_id,balance_after,description,created_at)
                VALUES (?,?,?,?,?,?,?)""",
                (sender_id, "payment_sent", -amount, receiver_id, ns,
                 f"Sent {amount:,} coins", now),
            )
            db.execute(
                """INSERT INTO transactions
                (user_id,type,amount,related_user_id,balance_after,description,created_at)
                VALUES (?,?,?,?,?,?,?)""",
                (receiver_id, "payment_received", amount, sender_id, nr,
                 f"Received {amount:,} coins", now),
            )
            db.execute("COMMIT")
            return True, "ok", ns, nr
        except Exception:
            db.execute("ROLLBACK")
            raise


async def timed_reward(user_id: int, column: str, cooldown: int, reward: int,
                      tx_type: str, description: str):
    allowed = {"daily_last", "work_last"}
    if column not in allowed:
        raise ValueError("Invalid cooldown column")
    await ensure_user(user_id)
    async with db_lock:
        db.execute("BEGIN IMMEDIATE")
        try:
            row = db.execute(
                f"SELECT coins,{column} AS last_claim FROM users WHERE user_id=?",
                (user_id,),
            ).fetchone()
            now = int(time.time())
            remaining = cooldown - (now - int(row["last_claim"]))
            if remaining > 0:
                db.execute("ROLLBACK")
                return False, int(row["coins"]), remaining
            new = int(row["coins"]) + reward
            db.execute(
                f"UPDATE users SET {column}=?,coins=? WHERE user_id=?",
                (now, new, user_id),
            )
            db.execute(
                """INSERT INTO transactions
                (user_id,type,amount,related_user_id,balance_after,description,created_at)
                VALUES (?,?,?,?,?,?,?)""",
                (user_id, tx_type, reward, None, new, description, now),
            )
            db.execute("COMMIT")
            return True, new, 0
        except Exception:
            db.execute("ROLLBACK")
            raise


async def top_users(limit=10):
    async with db_lock:
        return db.execute(
            "SELECT user_id,coins,level FROM users WHERE coins>0 ORDER BY coins DESC,user_id ASC LIMIT ?",
            (limit,),
        ).fetchall()


async def recent_transactions(user_id: int, limit=10):
    await ensure_user(user_id)
    async with db_lock:
        return db.execute(
            "SELECT * FROM transactions WHERE user_id=? ORDER BY id DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()


async def get_baltop_configs():
    async with db_lock:
        return db.execute("SELECT * FROM baltop_config").fetchall()


async def save_baltop(guild_id, channel_id, message_id):
    async with db_lock:
        db.execute(
            """INSERT INTO baltop_config(guild_id,channel_id,message_id,updated_at)
            VALUES(?,?,?,?) ON CONFLICT(guild_id) DO UPDATE SET
            channel_id=excluded.channel_id,message_id=excluded.message_id,updated_at=excluded.updated_at""",
            (guild_id, channel_id, message_id, int(time.time())),
        )


async def save_level_config(guild_id, channel_id, enabled):
    async with db_lock:
        db.execute(
            """INSERT INTO level_config(guild_id,channel_id,enabled) VALUES(?,?,?)
            ON CONFLICT(guild_id) DO UPDATE SET channel_id=excluded.channel_id,enabled=excluded.enabled""",
            (guild_id, channel_id, 1 if enabled else 0),
        )


async def get_level_config(guild_id):
    async with db_lock:
        return db.execute("SELECT * FROM level_config WHERE guild_id=?", (guild_id,)).fetchone()

# ============================================================
# LEVELING
# ============================================================

def required_messages(level: int) -> int:
    """Cumulative message target for the given level."""
    if level <= 0:
        return 0
    if level in LEVEL_MILESTONES:
        return LEVEL_MILESTONES[level]
    # After level 3, add 25 messages per level.
    return 100 + (level - 3) * 25


def level_reward(level: int) -> int:
    return LEVEL_REWARD_BASE + (level - 1) * LEVEL_REWARD_STEP


async def process_xp(user_id: int, content: str):
    text = " ".join(content.strip().lower().split())
    if len(text) < MIN_MESSAGE_LENGTH:
        return None
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    now = int(time.time())
    await ensure_user(user_id)

    async with db_lock:
        db.execute("BEGIN IMMEDIATE")
        try:
            row = db.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()
            last_xp = int(row["xp_last"])
            last_hash = row["last_message_hash"]
            last_seen = int(row["last_message_at"])

            if last_hash == digest and now - last_seen < REPEAT_MESSAGE_COOLDOWN:
                db.execute("ROLLBACK")
                return None

            if now - last_xp < XP_COOLDOWN:
                db.execute(
                    "UPDATE users SET last_message_hash=?,last_message_at=? WHERE user_id=?",
                    (digest, now, user_id),
                )
                db.execute("COMMIT")
                return None

            old_level = int(row["level"])
            progress = int(row["level_messages"]) + 1
            total = int(row["total_messages"]) + 1
            new_level = old_level

            if old_level < 100 and progress >= required_messages(old_level + 1):
                new_level = old_level + 1

            db.execute(
                """UPDATE users SET level=?,level_messages=?,total_messages=?,xp_last=?,
                last_message_hash=?,last_message_at=? WHERE user_id=?""",
                (new_level, progress, total, now, digest, now, user_id),
            )
            db.execute("COMMIT")
        except Exception:
            db.execute("ROLLBACK")
            raise

    if new_level == old_level:
        return None

    reward = level_reward(new_level)
    ok, _ = await change_balance(user_id, reward, "level_up", f"Reached level {new_level}")
    if not ok:
        return None

    if new_level == 100:
        async with db_lock:
            db.execute(
                "UPDATE users SET level=0,level_messages=0 WHERE user_id=?",
                (user_id,),
            )
        return {"level": 100, "reward": reward, "reset": True, "progress": 0}

    return {"level": new_level, "reward": reward, "reset": False, "progress": progress}

# ============================================================
# FORMATTING / EMBEDS
# ============================================================

def coins(n: int) -> str:
    return f"{int(n):,}"


def cooldown_text(seconds: int) -> str:
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    parts = []
    if h: parts.append(f"{h}h")
    if m: parts.append(f"{m}m")
    if s or not parts: parts.append(f"{s}s")
    return " ".join(parts)


def error_embed(title: str, text: str):
    return discord.Embed(title=f"⚠️ {title}", description=text, color=discord.Color.red())


def balance_embed(user, row):
    level = int(row["level"])
    progress = int(row["level_messages"])
    if level >= 100:
        prog = "MAX"
    else:
        prog = f"{progress:,}/{required_messages(level + 1):,}"
    e = discord.Embed(title="💰 Balance", color=discord.Color.blurple())
    e.set_author(name=str(user), icon_url=user.display_avatar.url)
    e.add_field(name="🪙 Wallet", value=f"**{coins(row['coins'])}**", inline=True)
    e.add_field(name="🏦 Bank", value=f"**{coins(row['bank'])}**", inline=True)
    e.add_field(name="📈 Level", value=f"**{level}**", inline=True)
    e.add_field(name="XP", value=f"**{prog}**", inline=True)
    e.add_field(name="💬 Total Messages", value=f"**{row['total_messages']:,}**", inline=True)
    e.set_footer(text="Global economy • same balance in every server")
    return e


def baltop_embed(rows):
    e = discord.Embed(
        title="🏆 GLOBAL COIN LEADERBOARD",
        description="Top richest users across every server using this bot.",
        color=discord.Color.gold(),
        timestamp=datetime.now(timezone.utc),
    )
    if not rows:
        e.description = "No users have coins yet."
        return e
    medals = ["🥇", "🥈", "🥉"]
    lines = []
    for i, row in enumerate(rows, 1):
        rank = medals[i - 1] if i <= 3 else f"`#{i}`"
        lines.append(f"{rank} <@{row['user_id']}> — **{coins(row['coins'])}** 🪙")
    e.add_field(name="Richest Players", value="\n".join(lines), inline=False)
    e.set_footer(text="Live global leaderboard")
    return e

# ============================================================
# HELP
# ============================================================

HELP_PAGES = [
    ("💰 Economy", [
        ("$bal [@user]", "Show a global balance."),
        ("$pay @user amount", "Send global coins to another user."),
        ("$daily", "Claim the daily reward."),
        ("$work", "Work for a random coin reward."),
        ("$deposit amount", "Move wallet coins into your global bank."),
        ("$withdraw amount", "Move bank coins back to your wallet."),
    ]),
    ("🎰 Games", [
        ("$coinflip amount heads/tails", "Fair 50/50 coinflip."),
        ("$slots amount", "Play the slot machine."),
        ("$duel @user", "Play a quick non-betting duel."),
        ("$dice", "Roll two dice and see your score."),
        ("$mine", "Play the safe-tile mine game."),
    ]),
    ("🏆 Leaderboards", [
        ("$baltop", "Show the global richest leaderboard."),
        ("$setbaltop #channel", "Create/update a live leaderboard (admin)."),
    ]),
    ("📈 Leveling", [
        ("$level [@user]", "Show level and message progress."),
        ("$setlevelchannel #channel", "Set level-up channel (admin)."),
        ("$levelchannel off", "Disable level-up messages (admin)."),
    ]),
    ("🧾 Transactions", [
        ("$transaction", "Show recent economy transactions."),
    ]),
    ("🛡️ Moderation", [
        ("$lockapps", "Disable public use of external/user-installed apps."),
        ("$unlockapps", "Allow public use of external/user-installed apps again."),
        ("$appguard", "Show the current external-app protection status."),
    ]),
    ("ℹ️ Info", [
        ("$help", "Open this button-based help menu."),
    ]),
]


class HelpView(discord.ui.View):
    def __init__(self, author_id: int):
        super().__init__(timeout=120)
        self.author_id = author_id
        self.page = 0

    def embed(self):
        title, cmds = HELP_PAGES[self.page]
        e = discord.Embed(
            title=f"📚 Help • {title}",
            description="Use the buttons to browse commands and examples.",
            color=discord.Color.blurple(),
        )
        for cmd, desc in cmds:
            e.add_field(name=f"`{cmd}`", value=desc, inline=False)
        e.set_footer(text=f"Page {self.page + 1}/{len(HELP_PAGES)}")
        return e

    async def interaction_check(self, interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                "❌ Only the user who opened this menu can control it.", ephemeral=True
            )
            return False
        return True

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True

    @discord.ui.button(label="Previous", emoji="◀", style=discord.ButtonStyle.secondary)
    async def prev(self, interaction, button):
        self.page = (self.page - 1) % len(HELP_PAGES)
        await interaction.response.edit_message(embed=self.embed(), view=self)

    @discord.ui.button(label="Next", emoji="▶", style=discord.ButtonStyle.primary)
    async def next(self, interaction, button):
        self.page = (self.page + 1) % len(HELP_PAGES)
        await interaction.response.edit_message(embed=self.embed(), view=self)

    @discord.ui.button(label="Close", emoji="❌", style=discord.ButtonStyle.danger)
    async def close(self, interaction, button):
        await interaction.response.edit_message(content="✅ Help menu closed.", embed=None, view=None)
        self.stop()

# ============================================================
# ANIMATION / GAME HELPERS
# ============================================================

async def animated_embed(ctx, final_embed, frames=("⏳ Loading…", "✨ Processing…"), delay=0.55):
    """Small edit-based animation: one message, no spam."""
    msg = await ctx.send(embed=discord.Embed(title=frames[0], color=discord.Color.blurple()))
    for frame in frames[1:]:
        await asyncio.sleep(delay)
        try:
            await msg.edit(embed=discord.Embed(title=frame, color=discord.Color.blurple()))
        except discord.HTTPException:
            pass
    await asyncio.sleep(delay)
    try:
        await msg.edit(embed=final_embed)
    except discord.HTTPException:
        pass
    return msg


def parse_amount(raw: Optional[str]):
    if raw is None:
        return None
    raw = raw.replace(",", "").replace("_", "")
    if not raw.isdigit() or int(raw) <= 0:
        return None
    return int(raw)


async def move_wallet_bank(user_id: int, amount: int, direction: str):
    await ensure_user(user_id)
    async with db_lock:
        db.execute("BEGIN IMMEDIATE")
        try:
            row = db.execute("SELECT coins,bank FROM users WHERE user_id=?", (user_id,)).fetchone()
            wallet, bank = int(row["coins"]), int(row["bank"])
            if direction == "deposit":
                if wallet < amount:
                    db.execute("ROLLBACK"); return False, wallet, bank
                wallet -= amount; bank += amount
                tx_amount = -amount; tx_type = "deposit"
            else:
                if bank < amount:
                    db.execute("ROLLBACK"); return False, wallet, bank
                bank -= amount; wallet += amount
                tx_amount = amount; tx_type = "withdraw"
            db.execute("UPDATE users SET coins=?,bank=? WHERE user_id=?", (wallet, bank, user_id))
            db.execute("INSERT INTO transactions(user_id,type,amount,related_user_id,balance_after,description,created_at) VALUES(?,?,?,?,?,?,?)",
                       (user_id, tx_type, tx_amount, None, wallet, f"{tx_type.title()} {amount:,} coins", int(time.time())))
            db.execute("COMMIT")
            return True, wallet, bank
        except Exception:
            db.execute("ROLLBACK"); raise


# ============================================================
# COMMANDS
# ============================================================

@bot.command(name="help")
async def help_cmd(ctx):
    view = HelpView(ctx.author.id)
    await ctx.send(embed=view.embed(), view=view)


@bot.command(name="bal", aliases=["balance"])
async def bal_cmd(ctx, member: Optional[discord.Member] = None):
    target = member or ctx.author
    row = await get_user(target.id)
    await ctx.send(embed=balance_embed(target, row), allowed_mentions=discord.AllowedMentions.none())


@bot.command(name="level", aliases=["lvl"])
async def level_cmd(ctx, member: Optional[discord.Member] = None):
    target = member or ctx.author
    row = await get_user(target.id)
    level = int(row["level"])
    e = discord.Embed(title="📈 Level", color=discord.Color.green())
    e.set_author(name=str(target), icon_url=target.display_avatar.url)
    e.add_field(name="Current Level", value=f"**{level}**", inline=True)
    if level >= 100:
        e.add_field(name="Progress", value="**MAX**", inline=True)
    else:
        target_messages = required_messages(level + 1)
        e.add_field(name="Progress", value=f"**{row['level_messages']:,}/{target_messages:,}**", inline=True)
        e.add_field(name="Remaining", value=f"**{max(0, target_messages - row['level_messages']):,}**", inline=True)
    e.add_field(name="Total Messages", value=f"**{row['total_messages']:,}**", inline=False)
    await ctx.send(embed=e, allowed_mentions=discord.AllowedMentions.none())


@bot.command(name="pay")
async def pay_cmd(ctx, member: Optional[discord.Member] = None, amount: Optional[str] = None):
    if member is None or amount is None:
        return await ctx.send(embed=error_embed("Usage", "`$pay @user amount`\nExample: `$pay @Friend 10000`"))
    value = amount.replace(",", "").replace("_", "")
    if not value.isdigit() or int(value) <= 0:
        return await ctx.send(embed=error_embed("Invalid Amount", "Use a positive whole number."))
    amount_int = int(value)
    ok, reason, sender, receiver = await transfer(ctx.author.id, member.id, amount_int)
    if not ok:
        msg = {
            "self": "You cannot pay yourself.",
            "amount": "Amount must be greater than 0.",
            "insufficient": f"You only have **{coins(sender)}** 🪙.",
        }.get(reason, "Payment failed.")
        return await ctx.send(embed=error_embed("Payment Failed", msg))
    e = discord.Embed(title="💸 Payment Successful", color=discord.Color.green())
    e.description = f"{ctx.author.mention} sent **{coins(amount_int)}** 🪙 to {member.mention}."
    e.add_field(name="Sender Balance", value=f"**{coins(sender)}** 🪙")
    e.add_field(name="Receiver Balance", value=f"**{coins(receiver)}** 🪙")
    await animated_embed(ctx, e, ("💸 Processing payment…", "✨ Transfer complete!"), 0.45)


@bot.command(name="daily")
async def daily_cmd(ctx):
    ok, new_balance, remaining = await timed_reward(
        ctx.author.id, "daily_last", DAILY_COOLDOWN, DAILY_REWARD, "daily", "Daily reward"
    )
    if not ok:
        return await ctx.send(embed=error_embed("Daily Already Claimed", f"Come back after **{cooldown_text(remaining)}**."))
    e = discord.Embed(
        title="🎁 Daily Reward",
        description=f"{ctx.author.mention} received **{coins(DAILY_REWARD)}** 🪙!\nBalance: **{coins(new_balance)}** 🪙",
        color=discord.Color.green(),
    )
    await animated_embed(ctx, e, ("🎁 Opening daily reward…", "✨ Reward unlocked!"))


@bot.command(name="work")
async def work_cmd(ctx):
    reward = random.randint(WORK_MIN, WORK_MAX)
    ok, new_balance, remaining = await timed_reward(
        ctx.author.id, "work_last", WORK_COOLDOWN, reward, "work", "Work reward"
    )
    if not ok:
        return await ctx.send(embed=error_embed("Already Worked", f"Come back after **{cooldown_text(remaining)}**."))
    e = discord.Embed(
        title="💼 Work Complete",
        description=f"{ctx.author.mention} earned **{coins(reward)}** 🪙!\nBalance: **{coins(new_balance)}** 🪙",
        color=discord.Color.green(),
    )
    await animated_embed(ctx, e, ("💼 Working…", "💰 Counting earnings…"))


@bot.command(name="deposit", aliases=["dep"])
async def deposit_cmd(ctx, amount: Optional[str] = None):
    value = parse_amount(amount)
    if value is None:
        return await ctx.send(embed=error_embed("Usage", "`$deposit amount`"))
    ok, wallet, bank = await move_wallet_bank(ctx.author.id, value, "deposit")
    if not ok:
        return await ctx.send(embed=error_embed("Not Enough Wallet Coins", f"Wallet: **{coins(wallet)}** 🪙"))
    e = discord.Embed(title="🏦 Deposit Complete", description=f"Moved **{coins(value)}** 🪙 to your bank.", color=discord.Color.green())
    e.add_field(name="🪙 Wallet", value=f"**{coins(wallet)}**", inline=True)
    e.add_field(name="🏦 Bank", value=f"**{coins(bank)}**", inline=True)
    await animated_embed(ctx, e, ("🏦 Opening bank…", "💰 Depositing…", "✅ Deposit complete!"), 0.4)


@bot.command(name="withdraw", aliases=["with"])
async def withdraw_cmd(ctx, amount: Optional[str] = None):
    value = parse_amount(amount)
    if value is None:
        return await ctx.send(embed=error_embed("Usage", "`$withdraw amount`"))
    ok, wallet, bank = await move_wallet_bank(ctx.author.id, value, "withdraw")
    if not ok:
        return await ctx.send(embed=error_embed("Not Enough Bank Coins", f"Bank: **{coins(bank)}** 🪙"))
    e = discord.Embed(title="💳 Withdraw Complete", description=f"Moved **{coins(value)}** 🪙 to your wallet.", color=discord.Color.green())
    e.add_field(name="🪙 Wallet", value=f"**{coins(wallet)}**", inline=True)
    e.add_field(name="🏦 Bank", value=f"**{coins(bank)}**", inline=True)
    await animated_embed(ctx, e, ("🏦 Opening bank…", "💸 Withdrawing…", "✅ Withdrawal complete!"), 0.4)


@bot.command(name="coinflip")
@commands.cooldown(1, GAME_COOLDOWN, commands.BucketType.user)
async def coinflip_cmd(ctx, amount: Optional[str] = None, choice: Optional[str] = None):
    if amount is None or choice is None:
        return await ctx.send(embed=error_embed("Usage", "`$coinflip 1000 heads`"))
    raw = amount.replace(",", "").replace("_", "")
    if not raw.isdigit() or int(raw) <= 0:
        return await ctx.send(embed=error_embed("Invalid Bet", "Use a positive whole number."))
    bet = int(raw)
    choice = {"h": "heads", "head": "heads", "t": "tails", "tail": "tails"}.get(choice.lower(), choice.lower())
    if choice not in ("heads", "tails"):
        return await ctx.send(embed=error_embed("Invalid Choice", "Choose `heads` or `tails`."))
    row = await get_user(ctx.author.id)
    if row["coins"] < bet:
        return await ctx.send(embed=error_embed("Not Enough Coins", f"You have **{coins(row['coins'])}** 🪙."))
    result = random.SystemRandom().choice(["heads", "tails"])
    if result == choice:
        ok, new_balance = await change_balance(ctx.author.id, bet, "coinflip_win", f"Coinflip: {result}")
        title, color, outcome = "🪙 COINFLIP • YOU WON!", discord.Color.green(), f"Profit: **+{coins(bet)}** 🪙"
    else:
        ok, new_balance = await change_balance(ctx.author.id, -bet, "coinflip_loss", f"Coinflip: {result}")
        title, color, outcome = "🪙 COINFLIP • YOU LOST", discord.Color.red(), f"Loss: **-{coins(bet)}** 🪙"
    if not ok:
        return await ctx.send(embed=error_embed("Error", "Try again."))
    e = discord.Embed(title=title, color=color)
    e.description = f"Result: **{result.upper()}**\n{outcome}\nBalance: **{coins(new_balance)}** 🪙"
    await animated_embed(ctx, e, ("🪙 Flipping…", "🔄 The coin is spinning…", "🪙 Result locked!"), 0.45)


@bot.command(name="slots")
@commands.cooldown(1, GAME_COOLDOWN, commands.BucketType.user)
async def slots_cmd(ctx, amount: Optional[str] = None):
    if amount is None:
        return await ctx.send(embed=error_embed("Usage", "`$slots 1000`"))
    raw = amount.replace(",", "").replace("_", "")
    if not raw.isdigit() or int(raw) <= 0:
        return await ctx.send(embed=error_embed("Invalid Bet", "Use a positive whole number."))
    bet = int(raw)
    row = await get_user(ctx.author.id)
    if row["coins"] < bet:
        return await ctx.send(embed=error_embed("Not Enough Coins", f"You have **{coins(row['coins'])}** 🪙."))

    result = random.choices(SLOT_SYMBOLS, weights=SLOT_WEIGHTS, k=3)
    payout = 0
    if result[0] == result[1] == result[2]:
        payout = bet * SLOT_TRIPLE_MULTIPLIER[result[0]]
    elif result[0] == result[1] or result[1] == result[2] or result[0] == result[2]:
        payout = bet * 2

    ok, balance_after_bet = await change_balance(ctx.author.id, -bet, "slots_bet", "Slots bet")
    if not ok:
        return await ctx.send(embed=error_embed("Error", "Try again."))
    if payout:
        ok, final_balance = await change_balance(ctx.author.id, payout, "slots_win", "Slots payout")
        if not ok:
            return await ctx.send(embed=error_embed("Error", "Payout failed."))
    else:
        final_balance = balance_after_bet

    profit = payout - bet
    e = discord.Embed(
        title="🎰 SLOTS • WIN!" if payout else "🎰 SLOTS • LOST",
        color=discord.Color.green() if payout else discord.Color.red(),
    )
    e.description = f"# {' | '.join(result)} #"
    e.add_field(name="Bet", value=f"**{coins(bet)}** 🪙")
    e.add_field(name="Result", value=f"**{profit:+,}** 🪙")
    e.add_field(name="Balance", value=f"**{coins(final_balance)}** 🪙")
    await animated_embed(ctx, e, ("🎰 Starting slots…", "🎰 Spinning…", "✨ Result!"), 0.4)


@bot.command(name="dice")
@commands.cooldown(1, GAME_COOLDOWN, commands.BucketType.user)
async def dice_cmd(ctx):
    a, b = random.randint(1, 6), random.randint(1, 6)
    total = a + b
    e = discord.Embed(title="🎲 DICE RESULT", description=f"{ctx.author.mention} rolled **{a} + {b} = {total}**", color=discord.Color.blurple())
    e.add_field(name="Score", value=f"**{total}/12**", inline=True)
    e.set_footer(text="3s game cooldown • no wagering")
    await animated_embed(ctx, e, ("🎲 Shaking dice…", "🎲 Rolling…", "✨ Dice stopped!"), 0.45)


@bot.command(name="mine")
@commands.cooldown(1, GAME_COOLDOWN, commands.BucketType.user)
async def mine_cmd(ctx):
    safe = random.randint(1, 9)
    cells = ["💎" if i == safe else "⬛" for i in range(1, 10)]
    grid = "\n".join(" ".join(cells[i:i+3]) for i in range(0, 9, 3))
    e = discord.Embed(title="💎 MINE SCAN", description=f"{grid}\n\nSafe tile: **#{safe}**", color=discord.Color.green())
    e.set_footer(text="3s game cooldown • no wagering")
    await animated_embed(ctx, e, ("⛏️ Scanning…", "🔎 Searching tiles…", "💎 Safe tile found!"), 0.45)


@bot.command(name="duel")
@commands.cooldown(1, GAME_COOLDOWN, commands.BucketType.user)
async def duel_cmd(ctx, member: Optional[discord.Member] = None):
    if member is None or member.bot or member.id == ctx.author.id:
        return await ctx.send(embed=error_embed("Usage", "`$duel @user` — choose another human player."))
    winner = random.choice([ctx.author, member])
    loser = member if winner.id == ctx.author.id else ctx.author
    e = discord.Embed(title="⚔️ DUEL COMPLETE", description=f"{winner.mention} wins the duel against {loser.mention}!", color=discord.Color.gold())
    e.add_field(name="Reward", value="🏆 Bragging rights + 1 duel win", inline=False)
    e.set_footer(text="3s game cooldown • no wagering")
    await animated_embed(ctx, e, ("⚔️ Duel starting…", "⚔️ Clash!", "🏆 Winner decided!"), 0.45)


@bot.command(name="transaction", aliases=["transactions", "tx"])
async def transaction_cmd(ctx):
    rows = await recent_transactions(ctx.author.id)
    e = discord.Embed(title="🧾 Recent Transactions", color=discord.Color.blurple())
    if not rows:
        e.description = "No transactions yet."
    else:
        lines = []
        for r in rows:
            amount = int(r["amount"])
            sign = "+" if amount > 0 else ""
            related = f" • <@{r['related_user_id']}>" if r["related_user_id"] else ""
            lines.append(f"**{r['type']}** `{sign}{coins(amount)}` 🪙 • <t:{r['created_at']}:R>{related}")
        e.description = "\n".join(lines)
    e.set_footer(text="Global transaction history")
    await ctx.send(embed=e, allowed_mentions=discord.AllowedMentions.none())


@bot.command(name="baltop", aliases=["leaderboard", "rich"])
async def baltop_cmd(ctx):
    await ctx.send(embed=baltop_embed(await top_users()))


@bot.command(name="lockapps")
@commands.has_guild_permissions(manage_guild=True)
@commands.bot_has_guild_permissions(manage_roles=True)
async def lockapps_cmd(ctx):
    """Disable public responses from user-installed external apps."""
    guild = ctx.guild
    if guild is None:
        return

    role = guild.default_role
    perms = role.permissions
    perms.use_external_apps = False

    try:
        await role.edit(permissions=perms, reason=f"External Apps Guard enabled by {ctx.author}")
    except discord.Forbidden:
        return await ctx.send(embed=error_embed(
            "Permission Error",
            "I need **Manage Roles** and must be above the @everyone role."
        ))
    except discord.HTTPException:
        return await ctx.send(embed=error_embed(
            "Discord Error", "Discord rejected the permission update. Try again."
        ))

    e = discord.Embed(
        title="🛡️ External Apps Guard • ON",
        description=(
            "Public use of **external/user-installed apps** is now disabled for normal members.\n\n"
            "• External app responses cannot be posted publicly by normal members\n"
            "• Server-installed apps are not automatically blocked\n"
            "• Members with **Administrator** can still bypass Discord permission rules"
        ),
        color=discord.Color.green(),
    )
    e.set_footer(text=f"Changed by {ctx.author}")
    await ctx.send(embed=e)


@bot.command(name="unlockapps")
@commands.has_guild_permissions(manage_guild=True)
@commands.bot_has_guild_permissions(manage_roles=True)
async def unlockapps_cmd(ctx):
    """Allow public responses from user-installed external apps again."""
    guild = ctx.guild
    if guild is None:
        return

    role = guild.default_role
    perms = role.permissions
    perms.use_external_apps = True

    try:
        await role.edit(permissions=perms, reason=f"External Apps Guard disabled by {ctx.author}")
    except discord.Forbidden:
        return await ctx.send(embed=error_embed(
            "Permission Error",
            "I need **Manage Roles** and must be above the @everyone role."
        ))
    except discord.HTTPException:
        return await ctx.send(embed=error_embed(
            "Discord Error", "Discord rejected the permission update. Try again."
        ))

    e = discord.Embed(
        title="🔓 External Apps Guard • OFF",
        description="Public use of external/user-installed apps is allowed again for members who have the Discord permission.",
        color=discord.Color.orange(),
    )
    e.set_footer(text=f"Changed by {ctx.author}")
    await ctx.send(embed=e)


@bot.command(name="appguard")
@commands.has_guild_permissions(manage_guild=True)
async def appguard_cmd(ctx):
    guild = ctx.guild
    if guild is None:
        return
    enabled = not guild.default_role.permissions.use_external_apps
    e = discord.Embed(
        title="🛡️ External Apps Guard",
        description=(
            "**ON** — normal members cannot publicly use external/user-installed apps."
            if enabled else
            "**OFF** — external/user-installed apps are allowed according to Discord permissions."
        ),
        color=discord.Color.green() if enabled else discord.Color.red(),
    )
    e.add_field(name="Public external apps", value="🔒 Blocked" if enabled else "🔓 Allowed", inline=True)
    e.add_field(name="Server-installed apps", value="Not changed", inline=True)
    e.set_footer(text="Use $lockapps or $unlockapps to change it")
    await ctx.send(embed=e)


@bot.command(name="setbaltop")
@commands.has_guild_permissions(manage_guild=True)
async def setbaltop_cmd(ctx, channel: Optional[discord.TextChannel] = None):
    channel = channel or ctx.channel
    if not channel.permissions_for(ctx.guild.me).send_messages:
        return await ctx.send(embed=error_embed("Missing Permission", f"I cannot send messages in {channel.mention}."))
    msg = await channel.send(embed=baltop_embed(await top_users()))
    await save_baltop(ctx.guild.id, channel.id, msg.id)
    await ctx.send(f"✅ Live `$baltop` enabled in {channel.mention}.")


@bot.command(name="setlevelchannel")
@commands.has_guild_permissions(manage_guild=True)
async def setlevelchannel_cmd(ctx, channel: Optional[discord.TextChannel] = None):
    channel = channel or ctx.channel
    await save_level_config(ctx.guild.id, channel.id, True)
    await ctx.send(f"✅ Level-up announcements will be sent in {channel.mention}.")


@bot.command(name="levelchannel")
@commands.has_guild_permissions(manage_guild=True)
async def levelchannel_cmd(ctx, mode: Optional[str] = None):
    if mode and mode.lower() == "off":
        await save_level_config(ctx.guild.id, None, False)
        await ctx.send("✅ Level-up announcements disabled.")
    else:
        await ctx.send(embed=error_embed("Usage", "`$levelchannel off` or `$setlevelchannel #channel`"))

# ============================================================
# PRESENCE / STATUS
# ============================================================

STATUS_TEXTS = [
    "$help | Global Economy",
    "$baltop | Richest Players",
    "🎰 Slots & Coinflip",
    "📈 Members Level Up",
    "💰 Global Economy",
    "$pay | $bal | $daily",
    "🏆 Global Leaderboard",
]
status_index = 0


@tasks.loop(seconds=STATUS_REFRESH)
async def status_loop():
    global status_index
    text = STATUS_TEXTS[status_index % len(STATUS_TEXTS)]
    status_index += 1
    try:
        await bot.change_presence(
            status=discord.Status.online,
            activity=discord.Activity(type=discord.ActivityType.watching, name=text),
        )
    except Exception:
        log.exception("Status update failed")


@status_loop.before_loop
async def status_before():
    await bot.wait_until_ready()

# ============================================================
# LIVE BALTOP
# ============================================================

@tasks.loop(seconds=BALTOP_REFRESH)
async def baltop_loop():
    configs = await get_baltop_configs()
    if not configs:
        return
    embed = baltop_embed(await top_users())
    for cfg in configs:
        guild = bot.get_guild(int(cfg["guild_id"]))
        if guild is None:
            continue
        channel = guild.get_channel(int(cfg["channel_id"]))
        if not isinstance(channel, discord.TextChannel):
            continue
        try:
            message = await channel.fetch_message(int(cfg["message_id"]))
            await message.edit(embed=embed)
        except discord.NotFound:
            try:
                new_msg = await channel.send(embed=embed)
                await save_baltop(guild.id, channel.id, new_msg.id)
            except discord.HTTPException:
                pass
        except (discord.Forbidden, discord.HTTPException):
            pass


@baltop_loop.before_loop
async def baltop_before():
    await bot.wait_until_ready()

# ============================================================
# LEVEL ANNOUNCEMENT
# ============================================================

async def announce_level(message: discord.Message, result: dict):
    config = await get_level_config(message.guild.id)
    if config is not None and not bool(config["enabled"]):
        return
    channel = message.channel
    if config is not None and config["channel_id"]:
        configured = message.guild.get_channel(int(config["channel_id"]))
        if isinstance(configured, discord.TextChannel):
            channel = configured

    if result["reset"]:
        e = discord.Embed(
            title="🏆 MAX LEVEL REACHED!",
            description=(
                f"{message.author.mention} reached **Level 100**!\n\n"
                f"🎁 Reward: **{coins(result['reward'])}** 🪙\n"
                "🔄 Level has been reset to **Level 0**.\n"
                "💰 Coins and transaction history were kept."
            ),
            color=discord.Color.gold(),
        )
    else:
        e = discord.Embed(
            title="🎉 LEVEL UP!",
            description=(
                f"{message.author.mention} reached **Level {result['level']}**!\n\n"
                f"🎁 Reward: **{coins(result['reward'])}** 🪙\n"
                f"💬 Progress: **{result['progress']:,}** valid messages"
            ),
            color=discord.Color.green(),
        )
    try:
        await channel.send(embed=e)
    except discord.HTTPException:
        pass

# ============================================================
# EVENTS
# ============================================================

@bot.event
async def on_ready():
    log.info("Logged in as %s (%s) | %d guild(s)", bot.user, bot.user.id, len(bot.guilds))
    if not status_loop.is_running():
        status_loop.start()
    if not baltop_loop.is_running():
        baltop_loop.start()
    await bot.change_presence(
        status=discord.Status.online,
        activity=discord.Activity(type=discord.ActivityType.watching, name=STATUS_TEXTS[0]),
    )


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or message.webhook_id:
        return

    if message.guild is not None and not message.content.lstrip().startswith(PREFIX):
        try:
            result = await process_xp(message.author.id, message.content)
            if result:
                await announce_level(message, result)
        except Exception:
            log.exception("XP processing error")

    await bot.process_commands(message)


@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        return
    if isinstance(error, commands.MissingPermissions):
        return await ctx.send(embed=error_embed("Permission Denied", "You need **Manage Server** permission."))
    if isinstance(error, commands.MissingRequiredArgument):
        return await ctx.send(embed=error_embed("Missing Argument", "Use `$help` for the correct usage."))
    if isinstance(error, commands.BadArgument):
        return await ctx.send(embed=error_embed("Invalid Argument", "Check `$help` and try again."))
    if isinstance(error, commands.CommandOnCooldown):
        return await ctx.send(embed=error_embed("Cooldown", f"Try again in **{cooldown_text(error.retry_after)}**."))
    log.exception("Command error", exc_info=error)
    try:
        await ctx.send(embed=error_embed("Unexpected Error", "Something went wrong. Please try again."))
    except discord.HTTPException:
        pass

# ============================================================
# RENDER HEALTH SERVER
# ============================================================

async def health_handler(request):
    return web.json_response({
        "status": "ok",
        "bot_ready": bot.is_ready(),
        "bot": str(bot.user) if bot.user else None,
        "guilds": len(bot.guilds),
    })


async def start_health_server():
    app = web.Application()
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info("Health server running on port %s", PORT)
    return runner

# ============================================================
# START
# ============================================================

async def main():
    if not TOKEN:
        raise RuntimeError("DISCORD_TOKEN is missing. Add it to Render Environment Variables.")
    runner = await start_health_server()
    try:
        await bot.start(TOKEN)
    finally:
        await runner.cleanup()
        db.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
