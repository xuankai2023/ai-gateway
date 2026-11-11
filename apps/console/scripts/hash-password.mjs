// 生成 CONSOLE_USERS 使用的 scrypt 密码哈希
// 用法：pnpm hash-password 'your-password'
import { randomBytes, scryptSync } from "node:crypto";

const password = process.argv[2];
if (!password) {
  console.error("用法: pnpm hash-password <密码>");
  process.exit(1);
}

const salt = randomBytes(16);
const hash = scryptSync(password, salt, 32);
console.log(`scrypt:${salt.toString("hex")}:${hash.toString("hex")}`);
