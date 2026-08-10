from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

import discord
from discord.ext import commands, tasks
import pymysql
from pymysql.cursors import DictCursor

from .unified_store import UnifiedDiscordStore, UnifiedState

log = logging.getLogger("funfernus-website-bridge")

RconCallback = Callable[[str], Awaitable[tuple[bool, str]]]


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _env_int(name: str, default: int) -> int:
    try:
        return int(_env(name, str(default)))
    except ValueError:
        return default


@dataclass(frozen=True)
class WebsiteDbConfig:
    host: str
    port: int
    database: str
    username: str
    password: str
    ssl_enabled: bool

    @property
    def ready(self) -> bool:
        return bool(self.host and self.database and self.username and self.password)

    @classmethod
    def from_env(cls) -> "WebsiteDbConfig":
        return cls(
            host=_env("WEB_DB_HOST"),
            port=_env_int("WEB_DB_PORT", 3306),
            database=_env("WEB_DB_NAME"),
            username=_env("WEB_DB_USER"),
            password=_env("WEB_DB_PASSWORD"),
            ssl_enabled=_env("WEB_DB_SSL", "true").lower() in {"1", "true", "yes", "on"},
        )


class WebsiteDatabase:
    """Small synchronous MySQL adapter. Calls are executed through asyncio.to_thread()."""

    def __init__(self, config: WebsiteDbConfig) -> None:
        self.config = config

    def connect(self):
        kwargs: dict[str, Any] = {
            "host": self.config.host,
            "port": self.config.port,
            "user": self.config.username,
            "password": self.config.password,
            "database": self.config.database,
            "charset": "utf8mb4",
            "autocommit": False,
            "cursorclass": DictCursor,
            "connect_timeout": 8,
            "read_timeout": 15,
            "write_timeout": 15,
        }
        if self.config.ssl_enabled:
            # PyMySQL enables TLS when ssl is a mapping. Certificate verification depends on
            # the provider's CA configuration; WEB_DB_SSL=false can be used only when the
            # hosting provider does not expose TLS.
            kwargs["ssl"] = {}
        return pymysql.connect(**kwargs)

    def test(self) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS ok")
                cur.fetchone()

    def claim_outbox(self) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM discord_outbox "
                    "WHERE status='pending' AND attempts < 8 ORDER BY id ASC LIMIT 1"
                )
                row = cur.fetchone()
                if not row:
                    conn.rollback()
                    return None
                cur.execute(
                    "UPDATE discord_outbox SET status='processing', attempts=attempts+1 "
                    "WHERE id=%s AND status='pending'",
                    (row["id"],),
                )
                if cur.rowcount != 1:
                    conn.rollback()
                    return None
                conn.commit()
                return row

    def mark_outbox_sent(self, outbox_id: int, discord_message_id: int | None = None) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE discord_outbox SET status='sent',sent_at=UTC_TIMESTAMP(),last_error=NULL,"
                    "discord_message_id=COALESCE(%s,discord_message_id) WHERE id=%s",
                    (str(discord_message_id) if discord_message_id else None, outbox_id),
                )
            conn.commit()

    def mark_outbox_retry(self, outbox_id: int, error: str) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT attempts FROM discord_outbox WHERE id=%s", (outbox_id,))
                row = cur.fetchone() or {"attempts": 8}
                status = "failed" if int(row["attempts"]) >= 8 else "pending"
                cur.execute(
                    "UPDATE discord_outbox SET status=%s,last_error=%s WHERE id=%s",
                    (status, error[:500], outbox_id),
                )
            conn.commit()

    def pending_application_messages(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id,discord_message_id FROM applications "
                    "WHERE status='pending' AND discord_message_id IS NOT NULL"
                )
                return list(cur.fetchall())

    def pending_public_application_messages(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id,discord_message_id FROM public_server_applications "
                    "WHERE status='pending' AND discord_message_id IS NOT NULL"
                )
                return list(cur.fetchall())

    def open_support_ticket_ids(self) -> list[int]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM support_tickets "
                    "WHERE status IN ('waiting_staff','waiting_user','in_progress','resolved')"
                )
                return [int(row["id"]) for row in cur.fetchall()]

    def get_application(self, app_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT a.*,u.minecraft_username,u.minecraft_uuid "
                    "FROM applications a JOIN users u ON u.id=a.user_id WHERE a.id=%s LIMIT 1",
                    (app_id,),
                )
                return cur.fetchone()

    def set_application_message(self, app_id: int, message_id: int, thread_id: int | None = None) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE applications SET discord_message_id=%s,discord_thread_id=COALESCE(%s,discord_thread_id) "
                    "WHERE id=%s",
                    (str(message_id), str(thread_id) if thread_id else None, app_id),
                )
            conn.commit()

    @staticmethod
    def _payload(row: dict[str, Any]) -> dict[str, Any]:
        raw = row.get("payload_json")
        if isinstance(raw, dict):
            return raw
        try:
            decoded = json.loads(raw or "{}")
            return decoded if isinstance(decoded, dict) else {}
        except (TypeError, json.JSONDecodeError):
            return {}

    @staticmethod
    def _slug(text: str, app_id: int) -> str:
        ascii_text = text.lower().replace("ё", "e")
        ascii_text = re.sub(r"[^a-z0-9]+", "-", ascii_text).strip("-")
        return (ascii_text[:60] or f"city-{app_id}")

    def approve_application(self, app_id: int, reviewer_id: int) -> tuple[bool, str, str]:
        """Return (ok, human_message, application_type)."""
        with self.connect() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT a.*,u.minecraft_username,u.minecraft_uuid "
                        "FROM applications a JOIN users u ON u.id=a.user_id "
                        "WHERE a.id=%s FOR UPDATE",
                        (app_id,),
                    )
                    row = cur.fetchone()
                    if not row:
                        conn.rollback()
                        return False, "Заявка не найдена.", "other"
                    app_type = str(row["application_type"])
                    if row["status"] != "pending":
                        conn.rollback()
                        return False, f"Заявка уже имеет статус: {row['status']}.", app_type

                    payload = self._payload(row)
                    result = "Заявка одобрена."

                    if app_type == "city":
                        cur.execute("SELECT 1 FROM city_members WHERE user_id=%s LIMIT 1", (row["user_id"],))
                        if cur.fetchone():
                            conn.rollback()
                            return False, "Игрок уже состоит в городе.", app_type
                        name = str(payload.get("entity_name") or row["title"] or "Новый город").strip()[:80]
                        description = str(payload.get("description") or "").strip()[:600]
                        slug_base = self._slug(name, app_id)
                        slug = slug_base
                        suffix = 1
                        while True:
                            cur.execute("SELECT 1 FROM cities WHERE slug=%s OR LOWER(name)=LOWER(%s) LIMIT 1", (slug, name))
                            if not cur.fetchone():
                                break
                            suffix += 1
                            slug = f"{slug_base[:55]}-{suffix}"
                        coordinates = str(payload.get("coordinates") or "").strip()[:255]
                        details = str(payload.get("details") or "").strip()[:1000]
                        discord_url = str(payload.get("discord_url") or "").strip()[:255] or None
                        cur.execute(
                            "INSERT INTO cities(slug,name,description,founder_user_id,founder_uuid,recruitment_status,discord_url,coordinates_text,details_text) "
                            "VALUES(%s,%s,%s,%s,%s,'applications',%s,%s,%s)",
                            (slug, name, description, row["user_id"], row["minecraft_uuid"], discord_url, coordinates, details),
                        )
                        city_id = int(cur.lastrowid)
                        role_defs = [
                            ("mayor", "Мэр", 100, 1),
                            ("deputy", "Заместитель мэра", 80, 1),
                            ("judge", "Судья", 60, 1),
                            ("lawyer", "Адвокат", 50, 1),
                            ("citizen", "Житель", 10, 1),
                        ]
                        role_ids: dict[str, int] = {}
                        for role_key, title, priority, system in role_defs:
                            cur.execute(
                                "INSERT INTO city_roles(city_id,role_key,title,priority,is_system) VALUES(%s,%s,%s,%s,%s)",
                                (city_id, role_key, title, priority, system),
                            )
                            role_ids[role_key] = int(cur.lastrowid)
                        cur.execute(
                            "INSERT INTO city_members(city_id,user_id,role_id) VALUES(%s,%s,%s)",
                            (city_id, row["user_id"], role_ids["mayor"]),
                        )
                        permissions = [
                            "post.create", "member.invite", "member.remove", "member.promote",
                            "role.manage", "treasury.view", "treasury.deposit", "treasury.withdraw",
                            "settings.edit", "recruitment.edit",
                        ]
                        allowed = {
                            "deputy": {"post.create", "member.invite", "member.remove", "member.promote", "treasury.view", "treasury.deposit", "settings.edit", "recruitment.edit"},
                            "judge": {"post.create", "treasury.view"},
                            "lawyer": {"post.create"},
                            "citizen": set(),
                        }
                        for role_key in ("deputy", "judge", "lawyer", "citizen"):
                            for permission in permissions:
                                cur.execute(
                                    "INSERT INTO city_role_permissions(role_id,permission_key,allowed) VALUES(%s,%s,%s)",
                                    (role_ids[role_key], permission, 1 if permission in allowed[role_key] else 0),
                                )
                        result = f"Город «{name}» создан и синхронизируется с Minecraft."

                    elif app_type == "business":
                        # Сайт создаёт бизнес сразу в статусе pending и списывает 192 АР.
                        # При одобрении мы активируем ЭТУ ЖЕ запись, а не создаём дубль.
                        business_id = int(payload.get("business_id") or 0)
                        name = str(payload.get("entity_name") or row["title"] or "Новый бизнес").strip()[:100]
                        description = str(payload.get("description") or "").strip()[:1000]
                        place_text = str(payload.get("place_text") or "").strip()[:255]
                        coordinates = str(payload.get("coordinates") or "").strip()[:255]
                        bank_account_id = int(payload.get("bank_account_id") or 0)
                        if business_id <= 0 or bank_account_id <= 0:
                            conn.rollback()
                            return False, "Данные бизнеса в заявке неполные.", app_type
                        cur.execute(
                            "SELECT id,status FROM businesses WHERE id=%s AND owner_user_id=%s FOR UPDATE",
                            (business_id, row["user_id"]),
                        )
                        business = cur.fetchone()
                        if not business:
                            conn.rollback()
                            return False, "Черновик бизнеса не найден в общей базе.", app_type
                        if str(business.get("status")) != "pending":
                            conn.rollback()
                            return False, f"Бизнес уже имеет статус: {business.get('status')}.", app_type
                        cur.execute(
                            "SELECT COUNT(*) AS c FROM businesses WHERE owner_user_id=%s AND id<>%s AND status IN ('pending','active')",
                            (row["user_id"], business_id),
                        )
                        if int((cur.fetchone() or {"c": 0})["c"]) >= 3:
                            conn.rollback()
                            return False, "У игрока уже достигнут лимит в 3 бизнеса.", app_type
                        cur.execute(
                            "SELECT id,account_number FROM bank_accounts WHERE id=%s AND owner_user_id=%s AND status='active' LIMIT 1",
                            (bank_account_id, row["user_id"]),
                        )
                        linked_account = cur.fetchone()
                        if not linked_account:
                            conn.rollback()
                            return False, "Выбранный банковский счёт больше недоступен владельцу.", app_type
                        plugin_id = f"webbiz-{business_id}"
                        cur.execute(
                            "UPDATE businesses SET plugin_business_id=%s,bank_account_id=%s,name=%s,description=%s,place_text=%s,planned_coordinates=%s,status='active',updated_at=UTC_TIMESTAMP() WHERE id=%s",
                            (plugin_id, bank_account_id, name, description, place_text, coordinates, business_id),
                        )
                        result = f"Бизнес «{name}» одобрен и активирован. Плагин подхватит его из общей БД."

                    cur.execute(
                        "UPDATE applications SET status='approved',review_comment=%s,reviewed_by_discord_id=%s,"
                        "reviewed_at=UTC_TIMESTAMP() WHERE id=%s",
                        (result, str(reviewer_id), app_id),
                    )
                    link_url = "/bank.php" if app_type == "business" else "/cities.php" if app_type == "city" else "/profile.php"
                    cur.execute(
                        "INSERT INTO notifications(user_id,notification_type,title,body,link_url) "
                        "VALUES(%s,'application','Заявка одобрена',%s,%s)",
                        (row["user_id"], result, link_url),
                    )
                conn.commit()
                return True, result, app_type
            except Exception:
                conn.rollback()
                raise

    def reject_application(self, app_id: int, reviewer_id: int, reason: str) -> tuple[bool, str]:
        reason = (reason.strip() or "Без указания причины")[:1000]
        with self.connect() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT a.user_id,a.status,a.application_type,a.payload_json,u.minecraft_uuid "
                        "FROM applications a JOIN users u ON u.id=a.user_id WHERE a.id=%s FOR UPDATE",
                        (app_id,),
                    )
                    row = cur.fetchone()
                    if not row:
                        conn.rollback()
                        return False, "Заявка не найдена."
                    if row["status"] != "pending":
                        conn.rollback()
                        return False, f"Заявка уже имеет статус: {row['status']}."
                    app_type = str(row.get("application_type") or "other")
                    payload = self._payload(row)
                    extra_note = ""
                    if app_type == "business":
                        business_id = int(payload.get("business_id") or 0)
                        fee_account_id = int(payload.get("fee_account_id") or 0)
                        fee_amount = int(payload.get("fee_amount") or 0)
                        if business_id > 0:
                            cur.execute(
                                "UPDATE businesses SET status='rejected',updated_at=UTC_TIMESTAMP() WHERE id=%s AND owner_user_id=%s AND status='pending'",
                                (business_id, row["user_id"]),
                            )
                        if fee_account_id > 0 and fee_amount > 0:
                            cur.execute(
                                "SELECT id,balance FROM bank_accounts WHERE id=%s AND owner_user_id=%s FOR UPDATE",
                                (fee_account_id, row["user_id"]),
                            )
                            account = cur.fetchone()
                            if account:
                                new_balance = int(account["balance"]) + fee_amount
                                cur.execute("UPDATE bank_accounts SET balance=%s WHERE id=%s", (new_balance, fee_account_id))
                                refund_op = f"business-refund-{app_id}"
                                cur.execute(
                                    "INSERT IGNORE INTO bank_transactions(operation_id,account_id,owner_uuid,type,amount,balance_after,description) "
                                    "VALUES(%s,%s,%s,'BUSINESS_APPLICATION_REFUND',%s,%s,%s)",
                                    (refund_op, fee_account_id, row["minecraft_uuid"], fee_amount, new_balance, f"Возврат за отклонённую заявку на бизнес #{app_id}"),
                                )
                                # Если INSERT IGNORE ничего не вставил (повтор), не оставляем повторное начисление.
                                if cur.rowcount != 1:
                                    cur.execute("UPDATE bank_accounts SET balance=balance-%s WHERE id=%s", (fee_amount, fee_account_id))
                                else:
                                    extra_note = f"  Возвращено {fee_amount} АР на основной счёт."
                    cur.execute(
                        "UPDATE applications SET status='rejected',review_comment=%s,reviewed_by_discord_id=%s,reviewed_at=UTC_TIMESTAMP() WHERE id=%s",
                        (reason, str(reviewer_id), app_id),
                    )
                    link_url = "/bank.php" if app_type == "business" else "/cities.php" if app_type == "city" else "/profile.php"
                    body = (reason + extra_note)[:1000]
                    cur.execute(
                        "INSERT INTO notifications(user_id,notification_type,title,body,link_url) "
                        "VALUES(%s,'application','Заявка отклонена',%s,%s)",
                        (row["user_id"], body, link_url),
                    )
                conn.commit()
                return True, reason + extra_note
            except Exception:
                conn.rollback()
                raise

    def public_application(self, application_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM public_server_applications WHERE id=%s LIMIT 1", (application_id,))
                return cur.fetchone()

    def set_public_application_message(self, app_id: int, message_id: int) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE public_server_applications SET discord_message_id=%s WHERE id=%s", (str(message_id), app_id))
            conn.commit()

    def approve_public_application(self, app_id: int, reviewer_id: int, rcon_note: str) -> tuple[bool, str, str]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM public_server_applications WHERE id=%s FOR UPDATE", (app_id,))
                row = cur.fetchone()
                if not row:
                    conn.rollback(); return False, "Заявка не найдена.", ""
                if row["status"] != "pending":
                    conn.rollback(); return False, f"Заявка уже имеет статус: {row['status']}.", str(row["minecraft_username"])
                comment = (rcon_note or "Игрок принят и добавлен в whitelist.")[:1000]
                cur.execute(
                    "UPDATE public_server_applications SET status='approved',review_comment=%s,reviewed_by_discord_id=%s,reviewed_at=UTC_TIMESTAMP() WHERE id=%s",
                    (comment, str(reviewer_id), app_id),
                )
            conn.commit()
            return True, comment, str(row["minecraft_username"])

    def reject_public_application(self, app_id: int, reviewer_id: int, reason: str) -> tuple[bool, str]:
        reason = (reason.strip() or "Без указания причины")[:1000]
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT status FROM public_server_applications WHERE id=%s FOR UPDATE", (app_id,))
                row = cur.fetchone()
                if not row:
                    conn.rollback(); return False, "Заявка не найдена."
                if row["status"] != "pending":
                    conn.rollback(); return False, f"Заявка уже имеет статус: {row['status']}."
                cur.execute(
                    "UPDATE public_server_applications SET status='rejected',review_comment=%s,reviewed_by_discord_id=%s,reviewed_at=UTC_TIMESTAMP() WHERE id=%s",
                    (reason, str(reviewer_id), app_id),
                )
            conn.commit()
            return True, reason

    def get_support_ticket(self, ticket_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT st.*,u.minecraft_username FROM support_tickets st "
                    "JOIN users u ON u.id=st.user_id WHERE st.id=%s LIMIT 1",
                    (ticket_id,),
                )
                return cur.fetchone()

    def set_support_thread(self, ticket_id: int, thread_id: int) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE support_tickets SET discord_thread_id=%s,status='waiting_staff' WHERE id=%s",
                    (str(thread_id), ticket_id),
                )
            conn.commit()

    def add_staff_support_message(self, thread_id: int, discord_id: int, body: str) -> tuple[bool, int, int]:
        body = body.strip()[:8000]
        if not body:
            return False, 0, 0
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id,user_id,ticket_number,status FROM support_tickets WHERE discord_thread_id=%s LIMIT 1",
                    (str(thread_id),),
                )
                ticket = cur.fetchone()
                if not ticket or ticket["status"] == "closed":
                    conn.rollback(); return False, 0, 0
                cur.execute(
                    "INSERT INTO support_messages(ticket_id,sender_type,sender_discord_id,body) VALUES(%s,'staff',%s,%s)",
                    (ticket["id"], str(discord_id), body),
                )
                cur.execute("UPDATE support_tickets SET status='waiting_user' WHERE id=%s", (ticket["id"],))
                cur.execute(
                    "INSERT INTO notifications(user_id,notification_type,title,body,link_url) "
                    "VALUES(%s,'support','Новый ответ техподдержки',%s,%s)",
                    (ticket["user_id"], body[:900], f"/support.php?ticket={ticket['id']}"),
                )
            conn.commit()
            return True, int(ticket["id"]), int(ticket["ticket_number"])

    def set_support_status(self, ticket_id: int, status: str) -> None:
        if status not in {"waiting_staff", "waiting_user", "in_progress", "resolved", "closed"}:
            return
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE support_tickets SET status=%s WHERE id=%s", (status, ticket_id))
            conn.commit()


class RejectReasonModal(discord.ui.Modal):
    def __init__(self, bridge: "WebsiteBridge", app_id: int, public: bool = False, source_message: discord.Message | None = None) -> None:
        super().__init__(title="Причина отклонения", timeout=300)
        self.bridge = bridge
        self.app_id = app_id
        self.public = public
        self.source_message = source_message
        self.reason = discord.ui.TextInput(
            label="Причина",
            style=discord.TextStyle.paragraph,
            min_length=2,
            max_length=1000,
            required=True,
        )
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not self.bridge.is_staff(interaction):
            await interaction.response.send_message("❌ Нет доступа.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        if self.public:
            ok, text = await asyncio.to_thread(
                self.bridge.db.reject_public_application,
                self.app_id,
                interaction.user.id,
                str(self.reason.value),
            )
        else:
            ok, text = await asyncio.to_thread(
                self.bridge.db.reject_application,
                self.app_id,
                interaction.user.id,
                str(self.reason.value),
            )
        if ok:
            await self.bridge.mark_review_message(self.source_message, False, text)
        await interaction.followup.send(("✅ " if ok else "❌ ") + text, ephemeral=True)


class WebsiteApplicationReviewView(discord.ui.View):
    def __init__(self, bridge: "WebsiteBridge", app_id: int, public: bool = False) -> None:
        super().__init__(timeout=None)
        self.bridge = bridge
        self.app_id = app_id
        self.public = public

        accept = discord.ui.Button(
            label="Принять",
            emoji="✅",
            style=discord.ButtonStyle.success,
            custom_id=f"ffweb:{'public' if public else 'app'}:approve:{app_id}",
        )
        reject = discord.ui.Button(
            label="Отклонить",
            emoji="🚫",
            style=discord.ButtonStyle.danger,
            custom_id=f"ffweb:{'public' if public else 'app'}:reject:{app_id}",
        )
        accept.callback = self._accept
        reject.callback = self._reject
        self.add_item(accept)
        self.add_item(reject)

    async def _accept(self, interaction: discord.Interaction) -> None:
        if not self.bridge.is_staff(interaction):
            await interaction.response.send_message("❌ Нет доступа.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        if self.public:
            row = await asyncio.to_thread(self.bridge.db.public_application, self.app_id)
            if not row:
                await interaction.followup.send("❌ Заявка не найдена.", ephemeral=True); return
            nickname = str(row["minecraft_username"])
            ok_rcon, rcon_text = await self.bridge.run_rcon(f"noblewl add name {nickname}")
            if not ok_rcon:
                await interaction.followup.send("❌ Whitelist не изменён: " + rcon_text, ephemeral=True); return
            ok, text, _ = await asyncio.to_thread(
                self.bridge.db.approve_public_application,
                self.app_id,
                interaction.user.id,
                rcon_text,
            )
        else:
            row = await asyncio.to_thread(self.bridge.db.get_application, self.app_id)
            if not row:
                await interaction.followup.send("❌ Заявка не найдена.", ephemeral=True); return
            if str(row["application_type"]) == "server":
                ok_rcon, rcon_text = await self.bridge.run_rcon(f"noblewl add name {row['minecraft_username']}")
                if not ok_rcon:
                    await interaction.followup.send("❌ Whitelist не изменён: " + rcon_text, ephemeral=True); return
            ok, text, _ = await asyncio.to_thread(
                self.bridge.db.approve_application,
                self.app_id,
                interaction.user.id,
            )
        if ok:
            await self.bridge.mark_review_message(interaction.message, True, text)
        await interaction.followup.send(("✅ " if ok else "❌ ") + text, ephemeral=True)

    async def _reject(self, interaction: discord.Interaction) -> None:
        if not self.bridge.is_staff(interaction):
            await interaction.response.send_message("❌ Нет доступа.", ephemeral=True)
            return
        await interaction.response.send_modal(RejectReasonModal(self.bridge, self.app_id, self.public, interaction.message))


class SupportReviewView(discord.ui.View):
    def __init__(self, bridge: "WebsiteBridge", ticket_id: int) -> None:
        super().__init__(timeout=None)
        self.bridge = bridge
        self.ticket_id = ticket_id
        resolve = discord.ui.Button(label="Решено", emoji="✅", style=discord.ButtonStyle.success, custom_id=f"ffweb:support:resolve:{ticket_id}")
        close = discord.ui.Button(label="Закрыть", emoji="🔒", style=discord.ButtonStyle.secondary, custom_id=f"ffweb:support:close:{ticket_id}")
        resolve.callback = self._resolve
        close.callback = self._close
        self.add_item(resolve); self.add_item(close)

    async def _resolve(self, interaction: discord.Interaction) -> None:
        if not self.bridge.is_staff(interaction):
            await interaction.response.send_message("❌ Нет доступа.", ephemeral=True); return
        await asyncio.to_thread(self.bridge.db.set_support_status, self.ticket_id, "resolved")
        await interaction.response.send_message("✅ Тикет отмечен решённым на сайте.", ephemeral=True)

    async def _close(self, interaction: discord.Interaction) -> None:
        if not self.bridge.is_staff(interaction):
            await interaction.response.send_message("❌ Нет доступа.", ephemeral=True); return
        await asyncio.to_thread(self.bridge.db.set_support_status, self.ticket_id, "closed")
        await interaction.response.send_message("🔒 Тикет закрыт на сайте.", ephemeral=True)
        if isinstance(interaction.channel, discord.Thread):
            try:
                await interaction.channel.edit(archived=True, locked=True, reason="Тикет закрыт через FunFernus Web Bridge")
            except discord.HTTPException:
                pass


class WebsiteBridge(commands.Cog):
    def __init__(
        self,
        bot: commands.Bot,
        store: UnifiedDiscordStore,
        admin_user_ids: set[int],
        run_rcon: RconCallback,
    ) -> None:
        self.bot = bot
        self.store = store
        self.admin_user_ids = set(admin_user_ids)
        self.run_rcon = run_rcon
        self.config = WebsiteDbConfig.from_env()
        self.db = WebsiteDatabase(self.config)
        self.public_url = _env("WEB_PUBLIC_URL", "https://funfernus.ru").rstrip("/")
        self.guild_id = _env_int("GUILD_ID", 0)
        self._rehydrated = False

    async def cog_load(self) -> None:
        if not self.config.ready:
            log.warning("Website bridge disabled: WEB_DB_HOST/NAME/USER/PASSWORD are not configured.")
            return
        try:
            await asyncio.to_thread(self.db.test)
        except Exception:
            log.exception("Website bridge cannot connect to MySQL; bot continues without web bridge.")
            return
        self.poll_outbox.start()
        log.info("Website bridge enabled: %s:%s/%s", self.config.host, self.config.port, self.config.database)

    def cog_unload(self) -> None:
        if self.poll_outbox.is_running():
            self.poll_outbox.cancel()

    async def state_for_guild(self, guild: discord.Guild) -> UnifiedState:
        return self.store.get(guild.id) or await self.store.load_or_create(guild)

    def is_staff(self, interaction: discord.Interaction) -> bool:
        guild = interaction.guild
        if guild is None:
            return False
        if guild.owner_id == interaction.user.id or interaction.user.id in self.admin_user_ids:
            return True
        if isinstance(interaction.user, discord.Member):
            state = self.store.get(guild.id)
            allowed = set(state.roles.get("support_staff", [])) if state else set()
            return any(role.id in allowed for role in interaction.user.roles)
        return False

    async def review_channel(self, guild: discord.Guild, app_type: str) -> discord.TextChannel | None:
        state = await self.state_for_guild(guild)
        channel_id = int(state.channels.get("city_review", 0)) if app_type == "city" else 0
        if not channel_id:
            settings = getattr(self.bot, "settings_store", None)
            current = settings.get_settings(guild.id) if settings else None
            channel_id = int(current.review_channel_id) if current else 0
        if not channel_id:
            channel_id = int(state.channels.get("support_review", 0))
        channel = guild.get_channel(channel_id) if channel_id else None
        if isinstance(channel, discord.TextChannel):
            return channel
        if channel_id:
            try:
                fetched = await self.bot.fetch_channel(channel_id)
                return fetched if isinstance(fetched, discord.TextChannel) else None
            except discord.HTTPException:
                return None
        return None

    async def support_channel(self, guild: discord.Guild) -> discord.TextChannel | None:
        state = await self.state_for_guild(guild)
        channel_id = int(state.channels.get("support_review", 0))
        channel = guild.get_channel(channel_id) if channel_id else None
        if isinstance(channel, discord.TextChannel):
            return channel
        if channel_id:
            try:
                fetched = await self.bot.fetch_channel(channel_id)
                return fetched if isinstance(fetched, discord.TextChannel) else None
            except discord.HTTPException:
                return None
        return await self.review_channel(guild, "support")

    async def target_guild(self) -> discord.Guild | None:
        if self.guild_id:
            return self.bot.get_guild(self.guild_id)
        return self.bot.guilds[0] if self.bot.guilds else None

    @staticmethod
    def parse_payload(raw: Any) -> dict[str, Any]:
        if isinstance(raw, dict):
            return raw
        try:
            value = json.loads(raw or "{}")
            return value if isinstance(value, dict) else {}
        except (TypeError, json.JSONDecodeError):
            return {}

    @tasks.loop(seconds=4.0)
    async def poll_outbox(self) -> None:
        if not self._rehydrated:
            await self.rehydrate_views()
            self._rehydrated = True
        row = await asyncio.to_thread(self.db.claim_outbox)
        if not row:
            return
        try:
            message_id = await self.process_outbox(row)
            await asyncio.to_thread(self.db.mark_outbox_sent, int(row["id"]), message_id)
        except Exception as exc:
            log.exception("Website outbox event %s failed", row.get("id"))
            await asyncio.to_thread(self.db.mark_outbox_retry, int(row["id"]), f"{type(exc).__name__}: {exc}")

    @poll_outbox.before_loop
    async def before_poll(self) -> None:
        await self.bot.wait_until_ready()

    async def rehydrate_views(self) -> None:
        try:
            rows = await asyncio.to_thread(self.db.pending_application_messages)
            for row in rows:
                mid = str(row.get("discord_message_id") or "")
                if mid.isdigit():
                    self.bot.add_view(WebsiteApplicationReviewView(self, int(row["id"])), message_id=int(mid))

            public_rows = await asyncio.to_thread(self.db.pending_public_application_messages)
            for row in public_rows:
                mid = str(row.get("discord_message_id") or "")
                if mid.isdigit():
                    self.bot.add_view(WebsiteApplicationReviewView(self, int(row["id"]), public=True), message_id=int(mid))

            # Support buttons use unique custom_id values, so restoring the persistent
            # views globally is sufficient even when the root Discord message id was
            # created by an older bridge version and is not stored in the site DB.
            ticket_ids = await asyncio.to_thread(self.db.open_support_ticket_ids)
            for ticket_id in ticket_ids:
                self.bot.add_view(SupportReviewView(self, ticket_id))
        except Exception:
            log.exception("Failed to restore website persistent views")

    async def process_outbox(self, row: dict[str, Any]) -> int | None:
        event = str(row["event_type"])
        payload = self.parse_payload(row.get("payload_json"))
        if event == "application.created":
            return await self.send_application(payload)
        if event == "public_application.created":
            return await self.send_public_application(payload)
        if event == "support.created":
            return await self.send_support_created(payload)
        if event == "support.message":
            return await self.send_support_message(payload)
        if event == "support.attachment":
            return await self.send_support_attachment(payload)
        log.info("Unknown website outbox event %s marked sent", event)
        return None

    async def send_application(self, payload: dict[str, Any]) -> int:
        guild = await self.target_guild()
        if guild is None:
            raise RuntimeError("Discord guild not found")
        app_id = int(payload.get("application_id") or 0)
        app_type = str(payload.get("type") or "other")
        channel = await self.review_channel(guild, app_type)
        if channel is None:
            raise RuntimeError("Application review channel is not configured")
        details = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
        embed = discord.Embed(
            title=f"📨 Заявка с сайта #{app_id}",
            description=str(payload.get("title") or "Новая заявка")[:4096],
            color=0xF2B84B,
        )
        embed.add_field(name="Тип", value=app_type, inline=True)
        embed.add_field(name="Minecraft", value=f"`{str(payload.get('minecraft_username') or '—')}`", inline=True)
        for title, key in (
            ("Название", "entity_name"),
            ("Описание", "description"),
            ("Планируемое место", "place_text"),
            ("Примерные координаты", "coordinates"),
            ("Банковский счёт", "bank_account_number"),
            ("Discord города", "discord_url"),
            ("Дополнительно", "details"),
        ):
            value = str(details.get(key) or "").strip()
            if value:
                embed.add_field(name=title, value=value[:1024], inline=key in {"bank_account_number"})
        embed.set_footer(text="FunFernus • Заявка создана на сайте")
        msg = await channel.send(embed=embed, view=WebsiteApplicationReviewView(self, app_id))
        await asyncio.to_thread(self.db.set_application_message, app_id, msg.id)
        return msg.id

    async def send_public_application(self, payload: dict[str, Any]) -> int:
        guild = await self.target_guild()
        if guild is None:
            raise RuntimeError("Discord guild not found")
        app_id = int(payload.get("application_id") or 0)
        channel = await self.review_channel(guild, "server")
        if channel is None:
            raise RuntimeError("Application review channel is not configured")
        embed = discord.Embed(title=f"📨 Заявка на сервер с сайта #{app_id}", color=0xF2B84B)
        labels = [
            ("🎮 Minecraft-ник", "minecraft_username"),
            ("🎂 Возраст", "age"),
            ("👋 О себе", "about"),
            ("💭 Почему FunFernus?", "why_funfernus"),
            ("🏗️ Планы на сервер", "plans"),
        ]
        for label, key in labels:
            value = str(payload.get(key) or "—")
            embed.add_field(name=label, value=value[:1024], inline=key in {"minecraft_username", "age"})
        embed.set_footer(text="Решение будет доступно заявителю на сайте по коду отслеживания")
        msg = await channel.send(embed=embed, view=WebsiteApplicationReviewView(self, app_id, public=True))
        await asyncio.to_thread(self.db.set_public_application_message, app_id, msg.id)
        return msg.id

    async def send_support_created(self, payload: dict[str, Any]) -> int:
        guild = await self.target_guild()
        if guild is None:
            raise RuntimeError("Discord guild not found")
        channel = await self.support_channel(guild)
        if channel is None:
            raise RuntimeError("Support review channel is not configured")
        ticket_id = int(payload.get("ticket_id") or 0)
        number = int(payload.get("ticket_number") or 0)
        embed = discord.Embed(
            title=f"🛟 Обращение №{number}",
            description=str(payload.get("body") or "")[:4096],
            color=0x19B9D1,
        )
        embed.add_field(name="Игрок", value=f"`{str(payload.get('minecraft_username') or '—')}`", inline=True)
        embed.add_field(name="Категория", value=str(payload.get("category") or "other")[:100], inline=True)
        embed.add_field(name="Тема", value=str(payload.get("subject") or "—")[:1024], inline=False)
        embed.set_footer(text="Ответьте сообщением в созданной ветке — ответ появится на сайте")
        msg = await channel.send(embed=embed, view=SupportReviewView(self, ticket_id))
        try:
            thread = await channel.create_thread(name=f"support-{number}-{str(payload.get('minecraft_username') or 'player')[:30]}", message=msg, auto_archive_duration=1440)
        except (discord.Forbidden, discord.HTTPException):
            thread = None
        if thread is not None:
            await asyncio.to_thread(self.db.set_support_thread, ticket_id, thread.id)
            await thread.send("💬 **Диалог с сайтом открыт.** Пишите ответы сюда обычными сообщениями.")
        return msg.id

    async def _support_thread(self, ticket_id: int, payload_thread_id: Any = None) -> discord.Thread | None:
        thread_id = str(payload_thread_id or "")
        if not thread_id.isdigit():
            ticket = await asyncio.to_thread(self.db.get_support_ticket, ticket_id)
            thread_id = str((ticket or {}).get("discord_thread_id") or "")
        if not thread_id.isdigit():
            return None
        channel = self.bot.get_channel(int(thread_id))
        if isinstance(channel, discord.Thread):
            return channel
        try:
            fetched = await self.bot.fetch_channel(int(thread_id))
            return fetched if isinstance(fetched, discord.Thread) else None
        except discord.HTTPException:
            return None

    async def send_support_message(self, payload: dict[str, Any]) -> int | None:
        ticket_id = int(payload.get("ticket_id") or 0)
        thread = await self._support_thread(ticket_id, payload.get("discord_thread_id"))
        if thread is None:
            raise RuntimeError("Support thread is not available")
        msg = await thread.send(f"🌐 **{str(payload.get('minecraft_username') or 'Игрок')} (сайт):**\n{str(payload.get('body') or '')[:1900]}")
        return msg.id

    async def send_support_attachment(self, payload: dict[str, Any]) -> int | None:
        ticket_id = int(payload.get("ticket_id") or 0)
        thread = await self._support_thread(ticket_id)
        if thread is None:
            raise RuntimeError("Support thread is not available")
        path = str(payload.get("public_path") or "").lstrip("/")
        if not path:
            return None
        # Only already-approved public media is linked. Quarantine paths are never sent to Discord.
        msg = await thread.send(f"🖼️ **Проверенное вложение с сайта:**\n{self.public_url}/{path}")
        return msg.id

    async def mark_review_message(self, message: discord.Message | None, accepted: bool, note: str) -> None:
        if message is None or not message.embeds:
            return
        embed = discord.Embed.from_dict(message.embeds[0].to_dict())
        embed.color = discord.Color(0x59B77A if accepted else 0xD85C5C)
        embed.add_field(name="Решение", value=("✅ Принято\n" if accepted else "🚫 Отклонено\n") + note[:900], inline=False)
        try:
            await message.edit(embed=embed, view=None)
        except discord.HTTPException:
            pass

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if not self.config.ready or message.author.bot or not isinstance(message.channel, discord.Thread):
            return
        guild = message.guild
        if guild is None:
            return
        # Only project staff can relay Discord messages back to the website.
        staff = guild.owner_id == message.author.id or message.author.id in self.admin_user_ids
        if isinstance(message.author, discord.Member):
            state = self.store.get(guild.id)
            role_ids = set(state.roles.get("support_staff", [])) if state else set()
            staff = staff or any(role.id in role_ids for role in message.author.roles)
        if not staff:
            return
        body = message.content.strip()
        if not body:
            return
        try:
            ok, ticket_id, ticket_number = await asyncio.to_thread(
                self.db.add_staff_support_message,
                message.channel.id,
                message.author.id,
                body,
            )
            if ok:
                try:
                    await message.add_reaction("🌐")
                except discord.HTTPException:
                    pass
                log.info("Support reply #%s relayed to website ticket %s", message.id, ticket_number)
        except Exception:
            log.exception("Could not relay Discord support reply to website")


async def setup_website_bridge(
    bot: commands.Bot,
    store: UnifiedDiscordStore,
    admin_user_ids: set[int],
    run_rcon: RconCallback,
) -> WebsiteBridge:
    bridge = WebsiteBridge(bot, store, admin_user_ids, run_rcon)
    await bot.add_cog(bridge)
    return bridge
