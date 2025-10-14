export const authRequired = (req, res, next) => {
  if (!req.session.user) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  req.user = req.session.user; // Almacena la información del usuario en req.user
  next();
};
