import nodemailer from "nodemailer";

const transporter = nodemailer.createTransport({
  service: "gmail",
  auth: {
    user: "ubikasite@gmail.com",
    pass: "tifg phvw fsgj hqsi", // Clave de aplicación
  },
});

// sendEmail.controller.js
const FRONTEND_URL = process.env.FRONTEND_URL || "http://localhost:3000";
const USE_HASH = true; // ponelo en false si usás BrowserRouter

export const sendConfirmationEmail = async ({ name, email, token }) => {
  const path = USE_HASH ? "/#/confirmar-email" : "/confirmar-email";
  const confirmUrl = `${FRONTEND_URL}${path}?token=${token}`;

  const mailOptions = {
    from: '"ValleyPass" <ubikasite@gmail.com>',
    to: email,
    subject: "Confirma tu cuenta en ValleyPass",
    html: `
      <h2>¡Bienvenido/a, ${name}!</h2>
      <p>Gracias por registrarte en ValleyPass.</p>
      <p>Confirmá tu correo haciendo clic en el siguiente enlace:</p>
      <p><a href="${confirmUrl}">Confirmar mi email</a></p>
      <p>Si no creaste esta cuenta, ignorá este mensaje.</p>
    `,
  };
  await transporter.sendMail(mailOptions);
};
